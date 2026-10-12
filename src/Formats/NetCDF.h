#pragma once

#include <base/types.h>

#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

class ReadBuffer;

/// Support for the NetCDF "classic" file formats: CDF-1 (the original classic format),
/// CDF-2 (the 64-bit offset format) and CDF-5 (the 64-bit data format).
/// See https://docs.unidata.ucar.edu/nug/current/file_format_specifications.html
///
/// The NetCDF-4 format is a completely different thing - it is an HDF5 file with a NetCDF
/// data model on top of it - and it is not supported here.

/// The element types of the format. The values are the ones stored in a file.
enum class NetCDFType : Int32
{
    Byte = 1,
    Char = 2,
    Short = 3,
    Int = 4,
    Float = 5,
    Double = 6,
    /// The types below exist only in CDF-5.
    UByte = 7,
    UShort = 8,
    UInt = 9,
    Int64 = 10,
    UInt64 = 11,
};

/// The size of one element of the type, in bytes.
size_t netCDFTypeSize(NetCDFType type);

/// The name of the type as it is spelled in the CDL language and in the specification.
const char * netCDFTypeName(NetCDFType type);

/// Whether the type can be stored only in CDF-5.
bool netCDFTypeRequiresCDF5(NetCDFType type);

/// Throws if the value is not one of the types above, or if it is a CDF-5 type in an older version.
NetCDFType checkNetCDFType(Int32 type, UInt8 version);

/// The longest name the reader accepts: anything above is taken as a sign of a corrupted or
/// hostile file. The writer enforces the same bound, or the file it produces could not be read back.
constexpr UInt64 NETCDF_MAX_NAME_LENGTH = 1 << 16;

/// The rules of a name of the classic format: a name is UTF-8 text; the first character is a
/// letter, a digit, an underscore or a character outside of ASCII; the rest are printable
/// characters other than a slash; and there are no trailing spaces.
/// See https://docs.unidata.ucar.edu/nug/current/file_format_specifications.html
/// Returns the reason why the name does not conform to them, or an empty string when it does.
/// The reader uses it so that a malformed file cannot publish an arbitrary byte string as a column
/// name, and the writer uses it so that it never produces a file its own reader rejects.
String checkNetCDFName(const String & name);

struct NetCDFDimension
{
    String name;
    /// The number of records for the unlimited dimension, and the declared length for the others.
    UInt64 length = 0;
    bool is_unlimited = false;
};

struct NetCDFAttribute
{
    String name;
    NetCDFType type = NetCDFType::Char;
    UInt64 num_elements = 0;
    /// The values in the big-endian representation, exactly as they are stored in the file.
    String data;
};

struct NetCDFVariable
{
    String name;
    NetCDFType type = NetCDFType::Double;
    std::vector<size_t> dimension_ids;
    std::vector<NetCDFAttribute> attributes;
    /// The offset of the data of the variable from the beginning of the file. For a record
    /// variable this is the offset of its slab inside the first record.
    UInt64 begin = 0;
    /// A variable is a record variable when its first dimension is the unlimited one.
    bool is_record = false;
    /// The number of bytes of one record of a record variable, or of the whole data of a
    /// non-record variable, without the padding.
    UInt64 slab_size = 0;

    const NetCDFAttribute * tryGetAttribute(std::string_view attribute_name) const;
};

struct NetCDFHeader
{
    UInt8 version = 1;
    /// The number of records is not stored in the file when it is written in the streaming mode,
    /// and has to be calculated from the size of the file.
    UInt64 num_records = 0;
    bool num_records_is_streaming = false;
    std::vector<NetCDFDimension> dimensions;
    std::vector<NetCDFAttribute> attributes;
    std::vector<NetCDFVariable> variables;
    std::optional<size_t> unlimited_dimension_id;
    /// The distance in bytes between the same slab of two consecutive records.
    UInt64 record_size = 0;
    /// The offset of the first record from the beginning of the file.
    UInt64 records_begin = 0;
    /// The size of the header in bytes.
    UInt64 size = 0;

    /// Calculates `num_records` from the size of the file for a file written in the streaming mode.
    void resolveNumberOfRecords(UInt64 file_size);
};

/// Reads the header of a NetCDF classic file. `in` must be positioned at the beginning of the file.
NetCDFHeader readNetCDFHeader(ReadBuffer & in);

/// The first bytes of a NetCDF-4 file, which is an HDF5 file. Used to report a clear error.
constexpr std::string_view HDF5_MAGIC = "\x89HDF\r\n\x1a\n";

}
