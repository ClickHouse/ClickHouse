#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <boost/noncopyable.hpp>
#if defined(__ELF__)
#include <Common/CompactSymbols.h>
#endif
#include <Common/Elf.h>
#if defined(__ELF__)
#include <IO/ZstdContext.h>
#endif

namespace DB
{

#if defined(__ELF__)
class CompactSymbolTable;
#endif

#if defined(OS_DARWIN)
/// Forward declaration to avoid pulling heavy MachO.h (and MMapReadBufferFromFile) into every includer.
class MachO;
#endif

/** Allow to quickly find symbol name from address.
  * Used as a replacement for "dladdr" function which is extremely slow.
  * It works better than "dladdr" because it also allows to search private symbols, that are not participated in shared linking.
  */
class SymbolIndex : private boost::noncopyable
{
protected:
    SymbolIndex() { load(); }

public:
    static const SymbolIndex & instance();

    struct Symbol
    {
        /// Here addresses are relative to objects.
        const void * offset_begin{};
        const void * offset_end{};

        static constexpr uint32_t invalid_source_id = std::numeric_limits<uint32_t>::max();

        void setNameReference(uint32_t source_id_, uint32_t offset_or_index_)
        {
            source_id = source_id_;
            offset_or_index = offset_or_index_;
        }

        uint32_t nameSourceId() const { return source_id; }
        uint32_t nameOffsetOrIndex() const { return offset_or_index; }

    private:
        /// Names are resolved through `Data::name_sources`. Direct sources interpret
        /// `offset_or_index` as a string-table offset; compact sources interpret it as a name index.
        uint32_t source_id = invalid_source_id;
        uint32_t offset_or_index{};
    };

    static_assert(sizeof(Symbol) == 24);

    struct NameSource
    {
        enum class Kind : uint8_t
        {
            Direct,
#if defined(__ELF__)
            Compact,
#endif
        };

        static NameSource direct(const char * base)
        {
            NameSource source;
            source.direct_base = base;
            return source;
        }

#if defined(__ELF__)
        static NameSource compact(uint32_t table_index)
        {
            NameSource source;
            source.kind = Kind::Compact;
            source.compact_table_index = table_index;
            return source;
        }
#endif

        Kind kind = Kind::Direct;
        union
        {
            const char * direct_base = nullptr;
#if defined(__ELF__)
            uint32_t compact_table_index;
#endif
        };
    };

    class SymbolIterator
    {
    public:
        /// Views returned by `next` point into member buffers, so moving the iterator
        /// could invalidate a previously returned view.
        SymbolIterator(const SymbolIterator &) = delete;
        SymbolIterator & operator=(const SymbolIterator &) = delete;
        SymbolIterator(SymbolIterator &&) = delete;
        SymbolIterator & operator=(SymbolIterator &&) = delete;

        /// `name` is NUL-terminated at `data()[size()]` and remains valid until the next call to `next`.
        bool next(const Symbol *& symbol, std::string_view & name);

    private:
        friend class SymbolIndex;
        explicit SymbolIterator(const SymbolIndex & index_);

        const SymbolIndex & index;
        size_t position = 0;
#if defined(__ELF__)
        uint32_t cached_source_id = std::numeric_limits<uint32_t>::max();
        uint32_t cached_granule_index = std::numeric_limits<uint32_t>::max();
        uint32_t cached_entry_index = std::numeric_limits<uint32_t>::max();
        ZstdDCtxPtr decompression_context;
        std::vector<char> granule_buffer;
        std::string name_buffer;
        std::optional<CompactSymbols::NameGranuleDecoder> decoder;
#endif
    };

    struct Object
    {
        /// Here addresses are absolute virtual memory addresses.
        const void * address_begin{};
        const void * address_end{};
        std::string name;
        std::shared_ptr<Elf> elf;
#if defined(OS_DARWIN)
        /// ASLR slide for this image. Subtract from runtime address to get linked (DWARF) address.
        uintptr_t slide = 0;
        /// Parsed dSYM bundle, if found next to the binary.
        std::shared_ptr<MachO> dsym;
#endif
    };

    const Symbol * findSymbol(const void * address) const;
    /// The returned view is NUL-terminated at `data()[size()]` and remains valid only until
    /// the next call to `getSymbolName` or an iterator method on the same thread. Copy it if needed longer.
    std::string_view getSymbolName(const Symbol & symbol) const;
    /// The returned pointer is never null and remains valid only until the next call to
    /// `getSymbolNameCString`, `getSymbolName`, or an iterator method on the same thread.
    /// An empty string is returned on failure. Copy it if needed longer.
    const char * getSymbolNameCString(const Symbol & symbol) const;
    /// Preallocates the compact-name workspace for the current thread.
    void warmUp() const;
    SymbolIterator iterateSymbols() const { return SymbolIterator(*this); }
    const Object * findObject(const void * address) const;
    const Object * thisObject() const;

    const std::vector<Symbol> & symbols() const { return data.symbols; }
    const std::vector<Object> & objects() const { return data.objects; }

    /// The BuildID that is generated by compiler.
    String getBuildID() const { return data.self_build_id; }
    String getBuildIDHex() const;

    struct Data
    {
        uint32_t registerNameSource(NameSource source)
        {
            if (name_sources.size() >= Symbol::invalid_source_id)
                return Symbol::invalid_source_id;

            uint32_t source_id = static_cast<uint32_t>(name_sources.size());
            name_sources.push_back(source);
            return source_id;
        }

        const NameSource * nameSource(const Symbol & symbol) const
        {
            if (symbol.nameSourceId() >= name_sources.size())
                return nullptr;
            return &name_sources[symbol.nameSourceId()];
        }

#if defined(__ELF__)
        bool hasCompactName(const Symbol & symbol) const
        {
            const NameSource * source = nameSource(symbol);
            return source && source->kind == NameSource::Kind::Compact;
        }
#endif

        std::vector<Symbol> symbols;
        std::vector<Object> objects;
        std::vector<NameSource> name_sources;
#if defined(__ELF__)
        std::vector<std::shared_ptr<const CompactSymbolTable>> compact_symbol_tables;
        bool has_compact_symbols = false;
#endif
        std::vector<uint32_t> symbol_scan_order;
        /// BuildID from the Object corresponding to main executable (as opposed to dynamic libraries).
        String self_build_id;
    };

private:
    Data data;
#if defined(__ELF__)
    size_t maximum_name_granule_size = 0;
#endif

    void load();
    const char * getSymbolNameImpl(const Symbol & symbol, size_t * name_size) const;
};

}
