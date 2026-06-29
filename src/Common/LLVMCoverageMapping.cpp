#if defined(__ELF__) && WITH_COVERAGE_DEPTH

#include <Common/LLVMCoverageMapping.h>

#include <elf.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>
#include <openssl/md5.h>

#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace
{

/// Compute MD5Hash the same way LLVM does: MD5 digest, return first 8 bytes as little-endian uint64.
/// This matches IndexedInstrProf::ComputeHash() which is used to compute FilenamesRef.
uint64_t computeMD5Hash(const uint8_t * data, size_t len)
{
    unsigned char digest[MD5_DIGEST_LENGTH]; // NOLINT
    MD5(data, len, digest); // NOLINT
    uint64_t result;
    memcpy(&result, digest, 8);
    return result;
}

/// Read an unsigned LEB128 value from buf, advance buf, return value.
/// Sets ok = false and returns 0 on truncated input or overflow.
uint64_t readULEB128(const uint8_t *& buf, const uint8_t * end, bool & ok)
{
    uint64_t result = 0;
    int shift = 0;
    while (buf < end)
    {
        const uint8_t byte = *buf++;
        result |= static_cast<uint64_t>(byte & 0x7fu) << shift;
        if (!(byte & 0x80u))
            return result;
        shift += 7;
        if (shift >= 64)
        {
            ok = false;
            return 0;
        }
    }
    ok = false;
    return 0;
}

/// One parsed filename table from a `__llvm_covmap` header block.
struct FilenameTable
{
    uint64_t hash;                    /// MD5Hash of the raw filename blob (= FilenamesRef in covfun records)
    std::vector<std::string> names;   /// decoded filenames
};

/// Parse all filename tables from the `__llvm_covmap` section.
/// Each block starts with a 16-byte header followed by FilenamesSize bytes of
/// compressed (or uncompressed) filename data.
///
/// Header layout (all uint32_t, little-endian):
///   NRecords     (always 0 for LLVM coverage format V4+)
///   FilenamesSize
///   CoverageSize (always 0 for V4+)
///   Version      (3 = V4, 4 = V5, 5 = V6, …)
///
/// The filename blob format (ULEB128 header + raw bytes):
///   NumFilenames   ULEB128
///   UncompressedLen ULEB128
///   CompressedLen   ULEB128
///   If CompressedLen > 0: zlib-compressed bytes
///   Else: raw uncompressed bytes
///
/// Uncompressed content for V5+ (i.e. format version 4+):
///   For each filename: ULEB128 length followed by that many bytes
std::vector<FilenameTable> parseCovMapFilenames(
    const uint8_t * covmap_data,
    size_t covmap_size)
{
    std::vector<FilenameTable> tables;

    const uint8_t * p = covmap_data;
    const uint8_t * const section_end = covmap_data + covmap_size;

    while (p < section_end)
    {
        /// Align start of each block to 8 bytes within the section.
        {
            const uintptr_t off = static_cast<uintptr_t>(p - covmap_data);
            if (off % 8 != 0)
                p += 8 - (off % 8);
        }
        if (p + 16 > section_end)
            break;

        uint32_t n_records;
        uint32_t filenames_size;
        uint32_t coverage_size;
        uint32_t version;
        memcpy(&n_records, p, 4);
        memcpy(&filenames_size, p + 4, 4);
        memcpy(&coverage_size, p + 8, 4);
        memcpy(&version, p + 12, 4);

        p += 16;

        if (p + filenames_size > section_end)
            break;

        const uint8_t * const fnames_start = p;
        const uint8_t * const fnames_end   = p + filenames_size;
        p = fnames_end;

        /// Compute MD5Hash of the raw filename blob — this is exactly what LLVM stores as FilenamesRef.
        const uint64_t block_hash = computeMD5Hash(fnames_start, filenames_size);

        /// Decode the filename blob.
        const uint8_t * fp = fnames_start;
        bool ok = true;

        const uint64_t num_filenames    = readULEB128(fp, fnames_end, ok);
        const uint64_t uncompressed_len = readULEB128(fp, fnames_end, ok);
        const uint64_t compressed_len   = readULEB128(fp, fnames_end, ok);
        if (!ok || num_filenames == 0)
            continue;

        /// Sanity-check the uncompressed size before allocating to avoid OOM on corrupt data.
        /// 256 MB is a generous upper bound; also require it is not implausibly large
        /// relative to the compressed/raw input (a 16× expansion factor is already very liberal).
        static constexpr uint64_t kMaxUncompressedLen = 256u * 1024u * 1024u;
        if (uncompressed_len > kMaxUncompressedLen
            || uncompressed_len > static_cast<uint64_t>(filenames_size) * 16)
            continue;

        std::string uncompressed;
        if (compressed_len > 0)
        {
            if (fp + compressed_len > fnames_end)
                continue;

            uncompressed.resize(uncompressed_len);
            uLongf dest_len = static_cast<uLongf>(uncompressed_len);
            if (uncompress(
                    reinterpret_cast<Bytef *>(uncompressed.data()),
                    &dest_len,
                    fp,
                    static_cast<uLong>(compressed_len)) != Z_OK)
                continue;

            fp += compressed_len;
        }
        else
        {
            if (fp + uncompressed_len > fnames_end)
                continue;
            uncompressed.assign(reinterpret_cast<const char *>(fp), uncompressed_len);
            fp += uncompressed_len;
        }

        /// Decode individual filenames.
        /// Format (V5 / format version ≥ 4): ULEB128 length then bytes.
        std::vector<std::string> filenames;
        filenames.reserve(static_cast<size_t>(num_filenames));

        const uint8_t * up  = reinterpret_cast<const uint8_t *>(uncompressed.data());
        const uint8_t * uend = up + uncompressed.size();

        for (uint64_t i = 0; i < num_filenames; ++i)
        {
            bool ok2 = true;
            const uint64_t len = readULEB128(up, uend, ok2);
            if (!ok2 || up + len > uend)
                break;
            filenames.emplace_back(reinterpret_cast<const char *>(up), static_cast<size_t>(len));
            up += len;
        }

        if (!filenames.empty())
            tables.push_back({block_hash, std::move(filenames)});
    }

    return tables;
}

} // anonymous namespace


std::vector<CoverageRegion> readLLVMCoverageMapping(const char * binary_path)
{
    std::vector<CoverageRegion> result;

    const int fd = ::open(binary_path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return result;

    struct stat st;
    if (::fstat(fd, &st) < 0)
    {
        [[maybe_unused]] int err = ::close(fd);
        return result;
    }

    const size_t size = static_cast<size_t>(st.st_size);
    void * mapped = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    [[maybe_unused]] int err = ::close(fd); /// mmap keeps the mapping valid after close
    if (mapped == MAP_FAILED)
        return result;

    const uint8_t * const base = static_cast<const uint8_t *>(mapped);

    auto cleanup = [&]
    {
        ::munmap(mapped, size);
    };

    /// Validate ELF magic and class (64-bit only).
    if (size < sizeof(Elf64_Ehdr)
        || memcmp(base, ELFMAG, SELFMAG) != 0
        || base[EI_CLASS] != ELFCLASS64)
    {
        cleanup();
        return result;
    }

    const Elf64_Ehdr * const elf = reinterpret_cast<const Elf64_Ehdr *>(base);

    if (elf->e_shoff == 0
        || elf->e_shstrndx == SHN_UNDEF
        || elf->e_shstrndx >= elf->e_shnum)
    {
        cleanup();
        return result;
    }

    /// Bounds check: verify section header table fits in the file.
    if (elf->e_shoff + static_cast<size_t>(elf->e_shnum) * sizeof(Elf64_Shdr) > size)
    {
        cleanup();
        return result;
    }

    const Elf64_Shdr * const shdrs =
        reinterpret_cast<const Elf64_Shdr *>(base + elf->e_shoff);

    /// Bounds check: verify the string table section offset is within the file.
    if (shdrs[elf->e_shstrndx].sh_offset >= size)
    {
        cleanup();
        return result;
    }

    const char * const shstrtab =
        reinterpret_cast<const char *>(base + shdrs[elf->e_shstrndx].sh_offset);
    const size_t shstrtab_size = static_cast<size_t>(shdrs[elf->e_shstrndx].sh_size);

    const uint8_t * covmap_data = nullptr;
    size_t covmap_size = 0;
    const uint8_t * covfun_data = nullptr;
    size_t covfun_size = 0;

    for (int i = 0; i < elf->e_shnum; ++i)
    {
        const Elf64_Shdr * const sh = &shdrs[i];

        /// Bounds check: sh_name must be within the string table.
        if (sh->sh_name >= shstrtab_size)
            continue;

        const char * const name = shstrtab + sh->sh_name;
        if (strcmp(name, "__llvm_covmap") == 0)
        {
            /// Bounds check: section data must fit in the file.
            if (sh->sh_offset > size || sh->sh_size > size - sh->sh_offset)
                continue;
            covmap_data = base + sh->sh_offset;
            covmap_size = static_cast<size_t>(sh->sh_size);
        }
        else if (strcmp(name, "__llvm_covfun") == 0)
        {
            /// Bounds check: section data must fit in the file.
            if (sh->sh_offset > size || sh->sh_size > size - sh->sh_offset)
                continue;
            covfun_data = base + sh->sh_offset;
            covfun_size = static_cast<size_t>(sh->sh_size);
        }
    }

    if (!covmap_data || !covfun_data)
    {
        cleanup();
        return result;
    }

    /// Parse filename tables from `__llvm_covmap`.
    const std::vector<FilenameTable> fname_tables =
        parseCovMapFilenames(covmap_data, covmap_size);

    if (fname_tables.empty())
    {
        cleanup();
        return result;
    }

    /// Build a lookup: MD5Hash → filename list.
    /// The hash matches FilenamesRef stored in covfun records.
    std::unordered_map<uint64_t, const FilenameTable *> hash_to_table;
    hash_to_table.reserve(fname_tables.size());
    for (const FilenameTable & t : fname_tables)
        hash_to_table.emplace(t.hash, &t);

    /// Parse `__llvm_covfun` records.
    ///
    /// Each record is LLVM_PACKED (no implicit padding) and 8-byte aligned:
    ///
    ///   int64_t  NameRef        [0..7]   MD5 hash of the mangled function name
    ///   uint32_t DataSize       [8..11]  byte length of the inline CoverageMapping
    ///   uint64_t FuncHash       [12..19] function body hash
    ///   uint64_t FilenamesRef   [20..27] MD5 hash of the filename blob in __llvm_covmap (format v5+)
    ///   uint8_t  CoverageMapping[DataSize] inline region encoding
    ///
    /// The inline CoverageMapping encoding (all ULEB128):
    ///   NumExpressions
    ///   2 × NumExpressions operands (LHS, RHS counter IDs — skipped)
    ///   Then one or more "file groups":
    ///     ULEB128  file_id_delta  (file index within the FilenamesRef table)
    ///     ULEB128  region_count   (number of regions that follow)
    ///     For each region:
    ///       ULEB128  delta_line_start
    ///       ULEB128  col_start
    ///       ULEB128  line_end_delta
    ///       ULEB128  col_end_combined  (col | (region_kind << 28))
    ///       ULEB128  counter_id
    ///
    /// We only need the first code region (region_kind == 0) of the first
    /// file group to obtain the source filename and line range.

    const uint8_t * p   = covfun_data;
    const uint8_t * end = covfun_data + covfun_size;

    while (p < end)
    {
        /// 8-byte alignment within the section.
        {
            const uintptr_t off = static_cast<uintptr_t>(p - covfun_data);
            if (off % 8 != 0)
                p += 8 - (off % 8);
        }
        if (p + 28 > end)
            break;

        int64_t  name_ref_raw;
        uint32_t data_size;
        uint64_t func_hash;
        uint64_t filenames_ref;

        memcpy(&name_ref_raw,  p,      8);
        memcpy(&data_size,     p + 8,  4);
        memcpy(&func_hash,     p + 12, 8);
        memcpy(&filenames_ref, p + 20, 8);

        const uint64_t name_hash = static_cast<uint64_t>(name_ref_raw);
        p += 28;

        if (p + data_size > end)
            break;

        const uint8_t * const mp   = p;
        const uint8_t * const mend = p + data_size;
        p += data_size;

        if (data_size == 0)
            continue;

        /// Decode the inline coverage mapping for this function.
        /// Format (all ULEB128):
        ///   1. NumFileMappings — number of files in virtual file mapping
        ///   2. NumFileMappings × FilenameIndex (index into FilenamesRef table)
        ///   3. NumExpressions
        ///   4. NumExpressions × 2 (LHS, RHS counter refs)
        ///   5. For each file (0..NumFileMappings-1):
        ///      a. NumRegions
        ///      b. For each region:
        ///         - EncodedCounterAndRegion (combined counter/kind)
        ///         - LineStartDelta (added to accumulator LineStart)
        ///         - ColumnStart
        ///         - NumLines
        ///         - ColumnEnd
        const uint8_t * cur = mp;
        bool ok = true;

        /// Step 1: virtual file mapping.
        /// Collect ALL file indices — the format has one region-block per file mapping entry,
        /// so regions for inlined code are attributed to the correct source file, not always
        /// the function's primary file.
        const uint64_t num_file_mappings = readULEB128(cur, mend, ok);
        if (!ok || num_file_mappings == 0)
            continue;

        std::vector<uint64_t> file_mapping_indices;
        file_mapping_indices.reserve(static_cast<size_t>(num_file_mappings));
        for (uint64_t fi = 0; fi < num_file_mappings; ++fi)
        {
            const uint64_t fname_idx = readULEB128(cur, mend, ok);
            if (!ok) break;
            file_mapping_indices.push_back(fname_idx);
        }
        if (!ok || file_mapping_indices.empty())
            continue;

        /// Step 2: skip expressions
        const uint64_t num_expr = readULEB128(cur, mend, ok);
        if (!ok)
            continue;
        for (uint64_t e = 0; e < num_expr * 2; ++e)
        {
            readULEB128(cur, mend, ok);
            if (!ok) break;
        }
        if (!ok || cur >= mend)
            continue;

        /// Resolve the filename table using FilenamesRef = MD5Hash of the covmap filename blob.
        auto it = hash_to_table.find(filenames_ref);
        if (it == hash_to_table.end())
            continue;

        const std::vector<std::string> & filenames = it->second->names;

        /// Step 3: iterate ALL file mappings.  Each mapping entry has its own NumRegions
        /// block so that regions for inlined functions are attributed to the correct file.
        /// LLVM encodes: for each file in the mapping [NumRegions][region_0]...[region_n]
        /// with line positions delta-encoded independently per file (cur_line resets to 0).
        ///
        /// Region encoding (each field is a ULEB128):
        ///   encoded          — counter/kind: tag=bits[1:0], counter_id=bits[63:2]
        ///                        tag==1: direct counter (counter_id = encoded >> 2)
        ///                        tag==0, kBranchKind: BranchRegion (true+false counters follow)
        ///                        tag==0, kMCDCDecisionKind/kMCDCBranchKind: MCDC — break, unneeded
        ///                        tag==0, other: gap/skipped/expansion — skip, 4 fields
        ///                        tag==2/3: expression counter — skip, 4 fields
        ///   line_start_delta — delta from previous region's line_start within this file block
        ///   col_start, num_lines, col_end_combined — discarded (only lines needed)

        static constexpr uint64_t kTagMask           = 3u;
        static constexpr uint64_t kExpansionBit      = 4u;
        /// EncodingCounterTagAndExpansionRegionTagBits = EncodingTagBits(2) + 1 = 3.
        /// Region kind is stored in EncodedCounterAndRegion >> 3 for tag-0 non-expansion regions.
        static constexpr uint64_t kKindShift         = 3u;
        static constexpr uint64_t kBranchKind        = 4u;
        static constexpr uint64_t kMCDCDecisionKind  = 5u;
        static constexpr uint64_t kMCDCBranchKind    = 6u;

        bool abort_function = false;
        for (uint64_t fmi = 0; fmi < num_file_mappings && !abort_function; ++fmi)
        {
            const uint64_t num_regions = readULEB128(cur, mend, ok);
            if (!ok) break;
            if (num_regions == 0) continue;

            /// Resolve filename for this specific file mapping entry.
            const size_t f_idx = static_cast<size_t>(file_mapping_indices[fmi]);
            if (f_idx >= filenames.size())
            {
                /// Unknown file index — break rather than attempt to skip with potentially wrong
                /// field counts (which would corrupt the cursor for subsequent file blocks).
                break;
            }
            const std::string & cur_filename = filenames[f_idx];

            /// Line positions delta-encoded independently per file block.
            uint32_t cur_line = 0;

            auto emit_region = [&](uint64_t counter_enc, uint32_t line_start, uint32_t line_end,
                                    bool is_branch, bool is_true_branch)
            {
                if ((counter_enc & kTagMask) != 1) return;
                if (line_start == 0) return;
                const uint32_t counter_id = static_cast<uint32_t>(counter_enc >> 2);
                CoverageRegion region;
                region.name_hash      = name_hash;
                region.func_hash      = func_hash;
                region.counter_id     = counter_id;
                region.is_branch      = is_branch;
                region.is_true_branch = is_true_branch;
                region.file           = cur_filename;
                region.line_start     = line_start;
                region.line_end       = line_end;
                result.push_back(std::move(region));
            };

        for (uint64_t ri = 0; ri < num_regions; ++ri)
        {
            const uint64_t encoded = readULEB128(cur, mend, ok);
            if (!ok) break;

            const uint64_t tag = encoded & kTagMask;

            /// BranchRegion: tag=0, not expansion, kind=kBranchKind.
            /// Format: [kind_marker] [true_counter] [false_counter] [line_delta] [col] [num_lines] [col_end]
            if (tag == 0 && !(encoded & kExpansionBit) && (encoded >> kKindShift) == kBranchKind)
            {
                /// BranchRegion: [kind=0x40][true_counter][false_counter]
                ///                [line_delta][col_start][num_lines][col_end]
                const uint64_t true_enc   = readULEB128(cur, mend, ok);
                const uint64_t false_enc  = readULEB128(cur, mend, ok);
                const uint64_t line_delta = readULEB128(cur, mend, ok);
                /* col_start = */           readULEB128(cur, mend, ok);
                const uint64_t num_lines  = readULEB128(cur, mend, ok);
                /* col_end = */             readULEB128(cur, mend, ok);
                if (!ok) break;
                cur_line += static_cast<uint32_t>(line_delta);
                const uint32_t ls = cur_line;
                const uint32_t le = cur_line + static_cast<uint32_t>(num_lines);
                emit_region(true_enc,  ls, le, /*is_branch=*/true,  /*is_true=*/true);
                emit_region(false_enc, ls, le, /*is_branch=*/true,  /*is_true=*/false);
                continue;
            }

            /// For all region kinds, the format starts with 4 standard position fields:
            ///   [line_delta] [col_start] [num_lines] [col_end]
            /// Some LLVM 17+ kinds (MCDCDecision, MCDCBranch) add extra fields AFTER.
            const uint64_t line_delta = readULEB128(cur, mend, ok);
            /* col_start = */           readULEB128(cur, mend, ok);
            const uint64_t num_lines  = readULEB128(cur, mend, ok);
            /* col_end = */             readULEB128(cur, mend, ok);
            if (!ok) break;

            cur_line += static_cast<uint32_t>(line_delta);

            /// MCDCDecisionRegion (kind=5) and MCDCBranchRegion (kind=6): LLVM 17+ MCDC.
            /// These have extra fields after the 4 standard position fields whose count
            /// varies by LLVM version.  Set abort_function so the fmi loop also stops —
            /// continuing with a misaligned cursor would corrupt all subsequent file mappings.
            if (tag == 0 && !(encoded & kExpansionBit))
            {
                const uint64_t kind = encoded >> kKindShift;
                if (kind == kMCDCDecisionKind || kind == kMCDCBranchKind)
                {
                    abort_function = true;
                    break;
                }
            }

            /// Only store direct-counter code regions (tag == 1).
            /// tag==0 non-branch: zero/skipped/gap/expansion — skip.
            /// tag==2/3: expression counters — skip (no direct counter id).
            if (tag != 1)
                continue;
            if (encoded & kExpansionBit)
                continue; /// expansion into another file

            emit_region(encoded, cur_line, cur_line + static_cast<uint32_t>(num_lines),
                        /*is_branch=*/false, /*is_true=*/false);
        }     /// end ri loop
        }     /// end fmi loop
    }         /// end while (p < end)

    cleanup();
    return result;
}

}

#endif
