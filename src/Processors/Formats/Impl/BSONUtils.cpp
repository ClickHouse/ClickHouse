#include <Processors/Formats/Impl/BSONUtils.h>

#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <Formats/FormatFactory.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>

#include <DataTypes/DataTypeDateTime64.h>

#include <base/strong_typedef.h>

namespace DB
{

namespace BSONUtils
{

    bool readField(
        ReadBuffer & in,
        IColumn & column,
        char type,
        UInt32 & already_read
    )
    {
        try
        {
            switch (type)
            {
                case BSONDataTypeIndex::DOUBLE:
                {
                    union {
                        char buf[BSON_64];
                        Float64 value;
                    } read_value;
                    in.read(read_value.buf, BSON_64);
                    assert_cast<ColumnFloat64 &>(column).insertValue(read_value.value);
                    already_read += BSON_64;
                    return true;
                }
                case BSONDataTypeIndex::STRING:
                {
                    union {
                        char buf[BSON_32];
                        UInt32 size;
                    } read_value;
                    in.read(read_value.buf, BSON_32);
                    already_read += BSON_32;
                    if (read_value.size != 0) {
                        String str;
                        str.resize(read_value.size - 1);
                        for (size_t i = 0; i + 1 < read_value.size; ++i)
                        {
                            in.read(str[i]);
                        }
                        assert_cast<ColumnString &>(column).insertData(str.c_str(), read_value.size - 1);
                        already_read += read_value.size;

                        char str_end;
                        in.read(str_end);
                        if (str_end != 0) throw Exception("Wrong BSON syntax", ErrorCodes::INCORRECT_DATA);
                    }
                    return true;
                }
                // case BSONDataTypeIndex::EMB_DOCUMENT: - Not supported
                case BSONDataTypeIndex::ARRAY:
                {
                    if (in.eof())
                        return false;

                    auto pre_array_size = already_read;

                    UInt32 arr_size;
                    {
                        union {
                            char buf[BSON_32];
                            UInt32 size;
                        } read_value;
                        in.read(read_value.buf, BSON_32);
                        arr_size = read_value.size;
                    }
                    already_read += BSON_32 + BSON_TYPE;

                    char nested_type;
                    in.read(nested_type);

                    ColumnArray & column_array = assert_cast<ColumnArray &>(column);
                    ColumnArray::Offsets & offsets = column_array.getOffsets();
                    IColumn & nested_column = column_array.getData();

                    size_t key_index = 0;
                    for (; already_read - pre_array_size < arr_size; ++key_index) {
                        char name_byte = 1;
                        while(name_byte != 0) {
                            in.read(name_byte);
                            ++already_read;
                        }
                        BSONUtils::readField(in, nested_column, nested_type, already_read);
                        if (already_read - pre_array_size + 1 >= arr_size) {
                            in.read(nested_type);
                            ++already_read;
                        }
                    }
                    offsets.push_back(offsets.back() + key_index);

                    return true;
                }
                // case BSONDataTypeIndex::BINARY:    - Not supported
                // case BSONDataTypeIndex::UNDEFINED: - Deprecated
                case BSONDataTypeIndex::OID:
                {
                    union {
                        char buf[BSON_128];
                        UInt128 value;
                    } read_value;
                    in.read(read_value.buf, BSON_128);
                    assert_cast<ColumnUUID &>(column).insertValue(StrongTypedef<UInt128, struct UUIDTag>(read_value.value));
                    already_read += BSON_128;
                    return true;
                }
                case BSONDataTypeIndex::BOOL:
                {
                    union {
                        char f;
                        UInt8 value;
                    } read_value;
                    in.read(read_value.f);
                    assert_cast<ColumnUInt8 &>(column).insertValue(read_value.value);
                    already_read += BSON_8;
                    return true;
                }
                case BSONDataTypeIndex::DATETIME:
                {
                    union {
                        char buf[BSON_64];
                        Int64 value;
                    } read_value;
                    in.read(read_value.buf, BSON_64);
                    assert_cast<DataTypeDateTime64::ColumnType &>(column).insertValue(read_value.value);
                    already_read += BSON_64;
                    return true;
                }
                case BSONDataTypeIndex::NULLVALUE:
                {
                    assert_cast<ColumnNullable &>(column).insertData(nullptr, 0);
                    return true;
                }
                // case BSONDataTypeIndex::REGEXP:                     - Not supported
                // case BSONDataTypeIndex::DBPOINTER:                  - Deprecated
                // case BSONDataTypeIndex::JAVASCRIPT_CODE:            - Not supported
                // case BSONDataTypeIndex::SYMBOL:                     - Deprecated
                // case BSONDataTypeIndex::JAVASCRIPT_CODE_WITH_SCOPE: - Deprecated
                case BSONDataTypeIndex::INT32:
                {
                    union {
                        char buf[BSON_32];
                        Int32 value;
                    } read_value;
                    in.read(read_value.buf, BSON_32);
                    assert_cast<ColumnInt32 &>(column).insertValue(read_value.value);
                    already_read += BSON_32;
                    return true;
                }
                case BSONDataTypeIndex::UINT64:
                {
                    union {
                        char buf[BSON_64];
                        UInt64 value;
                    } read_value;
                    in.read(read_value.buf, BSON_64);
                    assert_cast<ColumnUInt64 &>(column).insertValue(read_value.value);
                    already_read += BSON_64;
                    return true;
                }
                case BSONDataTypeIndex::INT64:
                {
                    union {
                        char buf[BSON_64];
                        Int64 value;
                    } read_value;
                    in.read(read_value.buf, BSON_64);
                    assert_cast<ColumnInt64 &>(column).insertValue(read_value.value);
                    already_read += BSON_64;
                    return true;
                }
                case BSONDataTypeIndex::DECIMAL128:
                {
                    union {
                        char buf[BSON_128];
                        Decimal128 value;
                    } read_value;
                    in.read(read_value.buf, BSON_128);
                    assert_cast<ColumnDecimal<Decimal128> &>(column).insertValue(read_value.value);
                    already_read += BSON_128;
                    return true;
                }
                // case BSONDataTypeIndex::MIN_KEY: - Not supported
                // case BSONDataTypeIndex::MAX_KEY: - Not supported
            }
        }
        catch (Exception &)
        {
            throw;
        }
        return false;
    }

}

}
