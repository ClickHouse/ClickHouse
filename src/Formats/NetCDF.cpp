#include <Formats/NetCDF.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>

#include <base/arithmeticOverflow.h>
#include <base/unit.h>

#include <algorithm>
#include <limits>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// The tags that introduce the lists in the header.
constexpr Int32 NC_DIMENSION = 10;
constexpr Int32 NC_VARIABLE = 11;
constexpr Int32 NC_ATTRIBUTE = 12;

/// Everything in the format is padded to a multiple of four bytes.
constexpr UInt64 ALIGNMENT = 4;

/// The values below are far above anything that can be found in a real file. They exist only to
/// stop a corrupted or hostile file from making us allocate an unreasonable amount of memory.
constexpr UInt64 MAX_LIST_SIZE = 1 << 24;
constexpr UInt64 MAX_VARIABLE_RANK = 1 << 16;
constexpr UInt64 MAX_ATTRIBUTE_DATA_SIZE = 256_MiB;
constexpr UInt64 MAX_HEADER_SIZE = 1_GiB;

UInt64 alignUpTo4(UInt64 size)
{
    return (size + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
}

/// A file is read only once and never seeked backwards while the header is being parsed, so the
/// number of bytes consumed so far is the current offset in the file.
void checkHeaderSize(ReadBuffer & in)
{
    if (in.count() > MAX_HEADER_SIZE)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The header of the NetCDF file is larger than {} bytes", MAX_HEADER_SIZE);
}

/// Reads `size` bytes without allocating `size` bytes up front, so that a bogus size in a corrupted
/// file leads to an "unexpected end of file" error instead of a huge allocation.
void readBytes(ReadBuffer & in, UInt64 size, String & out)
{
    static constexpr UInt64 step = 1_MiB;

    out.clear();
    for (UInt64 read_so_far = 0; read_so_far < size;)
    {
        UInt64 to_read = std::min(step, size - read_so_far);
        size_t old_size = out.size();
        out.resize(old_size + to_read);
        in.readStrict(out.data() + old_size, to_read);
        read_so_far += to_read;
    }
}

void skipPadding(ReadBuffer & in, UInt64 size)
{
    in.ignore(alignUpTo4(size) - size);
}

/// A four byte big-endian signed integer, which is used for the tags and for the types.
Int32 readTag(ReadBuffer & in)
{
    Int32 value = 0;
    readBinaryBigEndian(value, in);
    return value;
}

/// `NON_NEG` in the specification: a 32-bit value in CDF-1 and CDF-2, and a 64-bit value in CDF-5.
UInt64 readSize(ReadBuffer & in, UInt8 version, const char * what)
{
    Int64 value = 0;
    if (version == 5)
    {
        readBinaryBigEndian(value, in);
    }
    else
    {
        Int32 value32 = 0;
        readBinaryBigEndian(value32, in);
        value = value32;
    }

    if (value < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Negative {} in the NetCDF header: {}", what, value);

    return static_cast<UInt64>(value);
}

/// Skips a `NON_NEG` field without looking at it. It is used for the declared size of a variable,
/// which is written as all ones for variables larger than 4 GiB in CDF-2, and would therefore be
/// rejected as negative by `readSize`.
void skipSize(ReadBuffer & in, UInt8 version)
{
    in.ignore(version == 5 ? 8 : 4);
}

/// `OFFSET` in the specification: a 32-bit value in CDF-1 and a 64-bit value in CDF-2 and CDF-5.
UInt64 readOffset(ReadBuffer & in, UInt8 version)
{
    Int64 value = 0;
    if (version == 1)
    {
        Int32 value32 = 0;
        readBinaryBigEndian(value32, in);
        value = value32;
    }
    else
    {
        readBinaryBigEndian(value, in);
    }

    if (value < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Negative offset in the NetCDF header: {}", value);

    return static_cast<UInt64>(value);
}

String readName(ReadBuffer & in, UInt8 version)
{
    UInt64 length = readSize(in, version, "name length");
    if (length > NETCDF_MAX_NAME_LENGTH)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The name in the NetCDF header is {} bytes long", length);

    String name;
    readBytes(in, length, name);
    skipPadding(in, length);
    checkHeaderSize(in);
    return name;
}

/// Reads the header of one of the three lists. Returns the number of elements in it.
/// An empty list is stored as a zero tag followed by a zero count.
UInt64 readListHeader(ReadBuffer & in, UInt8 version, Int32 expected_tag, const char * what)
{
    Int32 tag = readTag(in);
    UInt64 num_elements = readSize(in, version, what);

    if (tag == 0)
    {
        if (num_elements != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The list of {} in the NetCDF header is marked as absent but declares {} elements", what, num_elements);
        return 0;
    }

    if (tag != expected_tag)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Unexpected tag {} instead of {} in the NetCDF header", tag, expected_tag);

    if (num_elements > MAX_LIST_SIZE)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The NetCDF header declares {} {}", num_elements, what);

    return num_elements;
}

std::vector<NetCDFAttribute> readAttributeList(ReadBuffer & in, UInt8 version)
{
    UInt64 num_attributes = readListHeader(in, version, NC_ATTRIBUTE, "attributes");

    std::vector<NetCDFAttribute> attributes;
    attributes.reserve(std::min<UInt64>(num_attributes, 1024));

    for (UInt64 i = 0; i < num_attributes; ++i)
    {
        NetCDFAttribute attribute;
        attribute.name = readName(in, version);
        attribute.type = checkNetCDFType(readTag(in), version);
        attribute.num_elements = readSize(in, version, "attribute element count");

        UInt64 data_size = 0;
        if (common::mulOverflow(attribute.num_elements, netCDFTypeSize(attribute.type), data_size)
            || data_size > MAX_ATTRIBUTE_DATA_SIZE)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The attribute {} of the NetCDF file declares {} elements", attribute.name, attribute.num_elements);
        }

        readBytes(in, data_size, attribute.data);
        skipPadding(in, data_size);
        checkHeaderSize(in);

        attributes.push_back(std::move(attribute));
    }

    return attributes;
}

}

size_t netCDFTypeSize(NetCDFType type)
{
    switch (type)
    {
        case NetCDFType::Byte: return 1;
        case NetCDFType::Char: return 1;
        case NetCDFType::Short: return 2;
        case NetCDFType::Int: return 4;
        case NetCDFType::Float: return 4;
        case NetCDFType::Double: return 8;
        case NetCDFType::UByte: return 1;
        case NetCDFType::UShort: return 2;
        case NetCDFType::UInt: return 4;
        case NetCDFType::Int64: return 8;
        case NetCDFType::UInt64: return 8;
    }
    return 0;
}

const char * netCDFTypeName(NetCDFType type)
{
    switch (type)
    {
        case NetCDFType::Byte: return "byte";
        case NetCDFType::Char: return "char";
        case NetCDFType::Short: return "short";
        case NetCDFType::Int: return "int";
        case NetCDFType::Float: return "float";
        case NetCDFType::Double: return "double";
        case NetCDFType::UByte: return "ubyte";
        case NetCDFType::UShort: return "ushort";
        case NetCDFType::UInt: return "uint";
        case NetCDFType::Int64: return "int64";
        case NetCDFType::UInt64: return "uint64";
    }
    return "unknown";
}

bool netCDFTypeRequiresCDF5(NetCDFType type)
{
    return type > NetCDFType::Double;
}

NetCDFType checkNetCDFType(Int32 type, UInt8 version)
{
    if (type < static_cast<Int32>(NetCDFType::Byte) || type > static_cast<Int32>(NetCDFType::UInt64))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown data type {} in the NetCDF header", type);

    auto result = static_cast<NetCDFType>(type);

    if (netCDFTypeRequiresCDF5(result) && version != 5)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "The data type {} of the NetCDF file is only allowed in CDF-5, but the file is CDF-{}",
            netCDFTypeName(result), static_cast<UInt16>(version));

    return result;
}

const NetCDFAttribute * NetCDFVariable::tryGetAttribute(std::string_view attribute_name) const
{
    auto it = std::find_if(attributes.begin(), attributes.end(),
        [&](const NetCDFAttribute & attribute) { return attribute.name == attribute_name; });
    return it == attributes.end() ? nullptr : &*it;
}

void NetCDFHeader::resolveNumberOfRecords(UInt64 file_size)
{
    if (!num_records_is_streaming)
        return;

    num_records_is_streaming = false;

    /// A file written in the streaming mode does not store the number of records, so the reader is
    /// expected to derive it from the size of the file.
    ///
    /// A file with zero records ends exactly where its first record variable begins: later record
    /// variables may point past the end of the file, but the earliest one cannot. A record section
    /// that begins past the end of the file means that the file was truncated inside the header
    /// padding or the data of the non-record variables.
    if (record_size != 0 && file_size < records_begin)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "The NetCDF file does not store the number of records and its record section begins at the offset {}, "
            "but the file is only {} bytes: the file is truncated", records_begin, file_size);

    if (record_size == 0 || file_size == records_begin)
    {
        num_records = 0;
    }
    else
    {
        UInt64 records_size = file_size - records_begin;

        /// The record section is the last thing in the file and has nothing after it, so the size
        /// of the file has to be an exact number of records. A remainder means that the file was
        /// truncated in the middle of a record, and reading the whole records before it would
        /// silently return fewer rows than the file was meant to have.
        if (records_size % record_size != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The NetCDF file does not store the number of records and its record section of {} bytes is not "
                "a whole number of records of {} bytes: the file is truncated", records_size, record_size);

        num_records = records_size / record_size;
    }

    if (unlimited_dimension_id)
        dimensions[*unlimited_dimension_id].length = num_records;
}

NetCDFHeader readNetCDFHeader(ReadBuffer & in)
{
    char magic[4];
    in.readStrict(magic, sizeof(magic));

    if (std::string_view(magic, sizeof(magic)) == HDF5_MAGIC.substr(0, sizeof(magic)))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "This is a NetCDF-4 file, which is stored in the HDF5 format. Only the NetCDF classic formats "
            "(CDF-1, CDF-2 and CDF-5) are supported. Convert the file with `nccopy -k cdf5 in.nc out.nc`");

    if (magic[0] != 'C' || magic[1] != 'D' || magic[2] != 'F')
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not a NetCDF file: the magic bytes are wrong");

    NetCDFHeader header;
    header.version = static_cast<UInt8>(magic[3]);

    if (header.version != 1 && header.version != 2 && header.version != 5)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Unknown version {} of the NetCDF classic format", static_cast<UInt16>(header.version));

    /// The number of records is written as all ones when the file is produced in the streaming mode.
    if (header.version == 5)
    {
        UInt64 num_records = 0;
        readBinaryBigEndian(num_records, in);
        /// The marker is all ones of the width of the field, so the 64-bit field of CDF-5 makes
        /// the 32-bit marker of the older versions a legal number of records.
        header.num_records_is_streaming = num_records == std::numeric_limits<UInt64>::max();
        header.num_records = header.num_records_is_streaming ? 0 : num_records;

        if (!header.num_records_is_streaming && num_records > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Negative number of records in the NetCDF header");
    }
    else
    {
        UInt32 num_records = 0;
        readBinaryBigEndian(num_records, in);
        header.num_records_is_streaming = num_records == std::numeric_limits<UInt32>::max();
        header.num_records = header.num_records_is_streaming ? 0 : num_records;
    }

    UInt64 num_dimensions = readListHeader(in, header.version, NC_DIMENSION, "dimensions");
    header.dimensions.reserve(std::min<UInt64>(num_dimensions, 1024));
    /// The set has to own the names: views into `header.dimensions` would dangle when the vector
    /// grows past the reserved size and reallocates.
    std::unordered_set<String> dimension_names;
    for (UInt64 i = 0; i < num_dimensions; ++i)
    {
        NetCDFDimension dimension;
        dimension.name = readName(in, header.version);
        dimension.length = readSize(in, header.version, "dimension length");

        /// A length of zero marks the unlimited dimension, of which there can be only one.
        if (dimension.length == 0)
        {
            if (header.unlimited_dimension_id)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "The NetCDF file has more than one unlimited dimension: {} and {}",
                    header.dimensions[*header.unlimited_dimension_id].name, dimension.name);

            dimension.is_unlimited = true;
            header.unlimited_dimension_id = i;
        }

        header.dimensions.push_back(std::move(dimension));

        if (!dimension_names.insert(header.dimensions.back().name).second)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The NetCDF file has more than one dimension named {}", header.dimensions.back().name);
    }

    if (header.unlimited_dimension_id)
        header.dimensions[*header.unlimited_dimension_id].length = header.num_records;

    header.attributes = readAttributeList(in, header.version);

    UInt64 num_variables = readListHeader(in, header.version, NC_VARIABLE, "variables");
    header.variables.reserve(std::min<UInt64>(num_variables, 1024));
    for (UInt64 i = 0; i < num_variables; ++i)
    {
        NetCDFVariable variable;
        variable.name = readName(in, header.version);

        UInt64 rank = readSize(in, header.version, "number of dimensions of a variable");
        if (rank > MAX_VARIABLE_RANK)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The variable {} of the NetCDF file has {} dimensions", variable.name, rank);

        variable.dimension_ids.reserve(rank);
        for (UInt64 j = 0; j < rank; ++j)
        {
            UInt64 dimension_id = readSize(in, header.version, "dimension index");
            if (dimension_id >= header.dimensions.size())
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "The variable {} of the NetCDF file refers to the dimension {}, but the file has only {} dimensions",
                    variable.name, dimension_id, header.dimensions.size());

            /// Only the first dimension of a variable is allowed to be the unlimited one.
            if (header.unlimited_dimension_id == dimension_id && j != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "The variable {} of the NetCDF file uses the unlimited dimension {} at the position {}",
                    variable.name, header.dimensions[dimension_id].name, j);

            variable.dimension_ids.push_back(dimension_id);
        }

        variable.attributes = readAttributeList(in, header.version);
        variable.type = checkNetCDFType(readTag(in), header.version);

        /// The declared size is redundant: it is the size of the data of the variable, or of one of
        /// its records, rounded up to four bytes. It is also unusable, because it is written as all
        /// ones for variables larger than 4 GiB in CDF-2, so the size is recalculated from the
        /// dimensions below. The field is skipped without validation, because the all-ones sentinel
        /// is a legitimate value that does not fit into the signed 32-bit field.
        skipSize(in, header.version);
        variable.begin = readOffset(in, header.version);

        variable.is_record = !variable.dimension_ids.empty() && header.unlimited_dimension_id == variable.dimension_ids[0];

        variable.slab_size = netCDFTypeSize(variable.type);
        for (size_t j = variable.is_record ? 1 : 0; j < variable.dimension_ids.size(); ++j)
        {
            if (common::mulOverflow(variable.slab_size, header.dimensions[variable.dimension_ids[j]].length, variable.slab_size))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "The size of the variable {} of the NetCDF file does not fit in 64 bits", variable.name);
        }

        checkHeaderSize(in);
        header.variables.push_back(std::move(variable));
    }

    header.size = in.count();

    /// The records of all record variables are interleaved: the file holds the slabs of every
    /// record variable for the record 0, then the same slabs for the record 1, and so on. Each slab
    /// is padded to four bytes, except in the special case of a file with a single record variable.
    size_t num_record_variables = 0;
    const NetCDFVariable * last_record_variable = nullptr;
    for (const auto & variable : header.variables)
    {
        if (!variable.is_record)
            continue;

        ++num_record_variables;
        last_record_variable = &variable;
        header.records_begin = num_record_variables == 1 ? variable.begin : std::min(header.records_begin, variable.begin);

        if (common::addOverflow(header.record_size, alignUpTo4(variable.slab_size), header.record_size))
            throw Exception(ErrorCodes::INCORRECT_DATA, "The size of a record of the NetCDF file does not fit in 64 bits");
    }

    if (num_record_variables == 1)
        header.record_size = last_record_variable->slab_size;

    for (const auto & variable : header.variables)
    {
        /// The data has to come after the header, or the bytes of the header itself would be
        /// served as the values of the variable.
        if (variable.begin < header.size)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The variable {} of the NetCDF file begins at the offset {}, inside the header of {} bytes",
                variable.name, variable.begin, header.size);

        /// The slab of a record variable has to fit inside the record, or reading it would return
        /// the bytes of another record.
        UInt64 slab_end = 0;
        if (variable.is_record
            && (common::addOverflow(variable.begin - header.records_begin, variable.slab_size, slab_end)
                || slab_end > header.record_size))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The variable {} of the NetCDF file begins at the offset {} of a record of {} bytes",
                variable.name, variable.begin - header.records_begin, header.record_size);
    }

    /// The slabs of the record variables inside a record also have to be disjoint, or two
    /// variables would silently serve the same bytes as their values.
    std::vector<const NetCDFVariable *> record_variables;
    for (const auto & variable : header.variables)
        if (variable.is_record)
            record_variables.push_back(&variable);

    std::sort(record_variables.begin(), record_variables.end(),
        [](const NetCDFVariable * lhs, const NetCDFVariable * rhs) { return lhs->begin < rhs->begin; });

    for (size_t i = 1; i < record_variables.size(); ++i)
    {
        const NetCDFVariable & previous = *record_variables[i - 1];
        const NetCDFVariable & next = *record_variables[i];

        /// The sum does not overflow: the loop above proved that it is at most record_size.
        if (previous.begin - header.records_begin + previous.slab_size > next.begin - header.records_begin)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The variables {} and {} of the NetCDF file overlap: their slabs begin at the offsets {} and {} "
                "of a record, and the first one is {} bytes",
                previous.name, next.name, previous.begin - header.records_begin,
                next.begin - header.records_begin, previous.slab_size);
    }

    /// The data of the fixed-size variables lives between the header and the records, and the
    /// payloads have to be disjoint for the same reason the slabs of the record variables do.
    /// A payload reaching past `records_begin` would additionally be counted as extra records
    /// when the number of records is derived from the file size in the streaming case.
    std::vector<const NetCDFVariable *> fixed_variables;
    for (const auto & variable : header.variables)
        if (!variable.is_record)
            fixed_variables.push_back(&variable);

    std::sort(fixed_variables.begin(), fixed_variables.end(),
        [](const NetCDFVariable * lhs, const NetCDFVariable * rhs) { return lhs->begin < rhs->begin; });

    UInt64 previous_end = 0;
    for (size_t i = 0; i < fixed_variables.size(); ++i)
    {
        const NetCDFVariable & variable = *fixed_variables[i];

        UInt64 end = 0;
        if (common::addOverflow(variable.begin, variable.slab_size, end))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The end of the variable {} of the NetCDF file does not fit in 64 bits", variable.name);

        if (num_record_variables != 0 && end > header.records_begin)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The variable {} of the NetCDF file ends at the offset {}, inside the records that begin at the offset {}",
                variable.name, end, header.records_begin);

        if (i != 0 && previous_end > variable.begin)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The variables {} and {} of the NetCDF file overlap: their data begins at the offsets {} and {} "
                "of the file, and the first one is {} bytes",
                fixed_variables[i - 1]->name, variable.name, fixed_variables[i - 1]->begin,
                variable.begin, fixed_variables[i - 1]->slab_size);

        previous_end = end;
    }

    return header;
}

}
