#include <Common/config.h>

#if USE_AEROSPIKE

#    include <sstream>
#    include <string>
#    include <vector>

#    include <aerospike/aerospike.h>
#    include <aerospike/aerospike_batch.h>
#    include <aerospike/aerospike_scan.h>
#    include <aerospike/as_batch.h>
#    include <aerospike/as_record.h>
#    include <aerospike/as_scan.h>
#    include <aerospike/as_val.h>

#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnsNumber.h>
#    include <IO/ReadHelpers.h>
#    include <IO/WriteHelpers.h>
#    include <Common/FieldVisitors.h>
#    include <ext/range.h>
#    include "AerospikeBlockInputStream.h"
#    include "DictionaryStructure.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

AerospikeBlockInputStream::AerospikeBlockInputStream(
    const aerospike & client, std::vector<std::unique_ptr<as_key>> && keys, const Block & sample_block, const size_t max_block_size)
    : client(client), keys(std::move(keys)), max_block_size{max_block_size}
{
    description.init(sample_block);
}

AerospikeBlockInputStream::~AerospikeBlockInputStream() = default;

namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    class TemporaryName
    {
    public:
        TemporaryName(MutableColumns * columns, const ExternalResultDescription & description, size_t cursor)
            : columns(columns), description(description), cursor(cursor)
        {
        }

        void HandleRecordBins(const as_record & record)
        {
            ++num_rows;
            for (const auto idx : ext::range(1, columns->size()))
            {
                const auto & name = description.sample_block.getByPosition(idx).name;

                const as_bin_value & bin_value = record.bins.entries[idx - 1].value;
                if (as_val_type(&bin_value) == AS_NIL)
                {
                    insertDefaultValue(*(*columns)[idx], *description.sample_block.getByPosition(idx).column);
                }
                else
                {
                    if (description.types[idx].second)
                    {
                        ColumnNullable & column_nullable = static_cast<ColumnNullable &>(*(*columns)[idx]);
                        insertValue(column_nullable.getNestedColumn(), description.types[idx].first, &bin_value, name);
                        column_nullable.getNullMapData().emplace_back(0);
                    }
                    else
                        insertValue(*(*columns)[idx], description.types[idx].first, &bin_value, name);
                }
            }
        }

        // Aerospike batch result return only bins. Keys must be processed separately
        void HandleKeys(const std::vector<std::unique_ptr<as_key>> & keys) const
        {
            const auto & name = description.sample_block.getByPosition(0).name; // MAY BE MOVE TO CLASS FIELDS
            for (const auto & key : keys)
            {
                insertKey(*((*columns)[0]), description.types[0].first, key.get(),
                          name); // TODO(gleb777) handle null result
            }
        }

        size_t getNumRows() const { return num_rows; }

    private:
        void insertKey(IColumn & column, const ValueType type, const as_key * key, const std::string & name) const
        {
            switch (type)
            {
                case ValueType::UInt8:
                    static_cast<ColumnVector<UInt8> &>(column).getData().push_back(key->value.integer.value);
                    break;
                case ValueType::UInt16:
                    static_cast<ColumnVector<UInt16> &>(column).getData().push_back(key->value.integer.value);
                    break;
                case ValueType::UInt32:
                    static_cast<ColumnVector<UInt32> &>(column).getData().push_back(key->value.integer.value);
                    break;
                case ValueType::UInt64:
                    static_cast<ColumnVector<UInt64> &>(column).getData().push_back(key->value.integer.value);
                    break;
                case ValueType::Int8:
                    static_cast<ColumnVector<Int8> &>(column).getData().push_back(key->value.integer.value);
                    break;
                case ValueType::Int16:
                    static_cast<ColumnVector<Int16> &>(column).getData().push_back(key->value.integer.value);
                    break;
                case ValueType::Int32:
                    static_cast<ColumnVector<Int32> &>(column).getData().push_back(key->value.integer.value);
                    break;
                case ValueType::Int64:
                    static_cast<ColumnVector<Int64> &>(column).getData().push_back(key->value.integer.value);
                    break;
                case ValueType::String:
                {
                    String str{key->value.string.value, key->value.string.len};
                    static_cast<ColumnString &>(column).insertDataWithTerminatingZero(str.data(), str.size() + 1);
                    break;
                }
                case ValueType::Date:
                    static_cast<ColumnUInt16 &>(column).getData().push_back(
                        UInt16{DateLUT::instance().toDayNum(static_cast<Int64>(key->value.integer.value))});
                    break;
                case ValueType::DateTime:
                    static_cast<ColumnUInt32 &>(column).getData().push_back(static_cast<Int64>(key->value.integer.value));
                    break;
                case ValueType::UUID:
                {
                    String str{key->value.string.value, key->value.string.len};
                    static_cast<ColumnUInt128 &>(column).getData().push_back(parse<UUID>(str));
                    break;
                }
                default:
                    std::string invalid_type = "bad"; // toString(as_val_type(&(key->value))); // TODO:FIX_ME(glebx777)
                    throw Exception{"Type mismatch, expected String (UUID), got type id = " + invalid_type + " for column " + name,
                                    ErrorCodes::TYPE_MISMATCH};
            }
        }

        template <typename T>
        void insertNumberValue(IColumn & column, const as_bin_value * value, const std::string & name)
        {
            switch (as_val_type(value))
            {
                case AS_INTEGER:
                    static_cast<ColumnVector<T> &>(column).getData().push_back(value->integer.value);
                    break;
                case AS_DOUBLE:
                    static_cast<ColumnVector<T> &>(column).getData().push_back(value->dbl.value);
                    break;
                    /*case AS_STRING:
                            static_cast<ColumnVector<T>&>(column).getData().push_back(value->string.value);
                            break; NOBODY USE IT */
                default:
                    std::string type = "bad"; // toString(as_val_type(&(value)); // TODO:FIX_ME(glebx777)
                    throw Exception(
                        "Type mismatch, expected a number, got type id = " + type + " for column " + name, ErrorCodes::TYPE_MISMATCH);
            }
        }

        template <typename T>
        inline void insert(IColumn & column, const String & stringValue)
        {
            static_cast<ColumnVector<T> &>(column).insertValue(parse<T>(stringValue));
        }

        void insertValue(IColumn & column, const ValueType type, const as_bin_value * value, const std::string & name)
        {
            switch (type)
            {
                case ValueType::UInt8:
                    insertNumberValue<UInt8>(column, value, name);
                    break;
                case ValueType::UInt16:
                    insertNumberValue<UInt16>(column, value, name);
                    break;
                case ValueType::UInt32:
                    insertNumberValue<UInt32>(column, value, name);
                    break;
                case ValueType::UInt64:
                    insertNumberValue<UInt64>(column, value, name);
                    break;
                case ValueType::Int8:
                    insertNumberValue<Int8>(column, value, name);
                    break;
                case ValueType::Int16:
                    insertNumberValue<Int16>(column, value, name);
                    break;
                case ValueType::Int32:
                    insertNumberValue<Int32>(column, value, name);
                    break;
                case ValueType::Int64:
                    insertNumberValue<Int64>(column, value, name);
                    break;
                case ValueType::Float32:
                    insertNumberValue<Float32>(column, value, name);
                    break;
                case ValueType::Float64:
                    insertNumberValue<Float64>(column, value, name);
                    break;
                case ValueType::String:
                {
                    String str{value->string.value, value->string.len};
                    static_cast<ColumnString &>(column).insertDataWithTerminatingZero(str.data(), str.size() + 1);
                    break;
                }
                case ValueType::Date:
                {
                    String str{value->string.value, value->string.len};
                    static_cast<ColumnUInt16 &>(column).insertValue(parse<LocalDate>(str).getDayNum());
                    break;
                }
                case ValueType::DateTime:
                {
                    String str{value->string.value, value->string.len};
                    static_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(parse<LocalDateTime>(str)));
                    break;
                }
                case ValueType::UUID:
                {
                    String str{value->string.value, value->string.len};
                    static_cast<ColumnUInt128 &>(column).insertValue(parse<UUID>(str));
                    break;
                }
                default:
                    std::string invalid_type = "bad"; // toString(as_val_type(&(value)); // TODO:FIX_ME(glebx777)
                    throw Exception{"Type mismatch, expected String (UUID), got type id = " + toString(invalid_type) + " for column "
                                        + name,
                                    ErrorCodes::TYPE_MISMATCH};
            }
        }

        void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }

        MutableColumns * columns;
        const ExternalResultDescription & description;
        size_t num_rows = 0;
    };
}

void InitializeBatchKey(as_key * new_key, const char * namespace_name, const char * set_name, const std::unique_ptr<as_key> & base_key)
{
    switch (as_val_type(&base_key->value))
    {
        case AS_INTEGER:
            as_key_init_int64(new_key, namespace_name, set_name, base_key->value.integer.value);
            break;
        case AS_STRING:
            as_key_init_str(new_key, namespace_name, set_name, base_key->value.string.value);
            break;
        default:
            const as_bytes & bytes = base_key->value.bytes;
            as_key_init_raw(new_key, "ns", "set", bytes.value, bytes.size);
            break;
    }
}

Block AerospikeBlockInputStream::readImpl()
{
    const size_t size = description.sample_block.columns();
    if (all_read || size == 0)
        return {};

    MutableColumns columns(size);
    for (const auto i : ext::range(0, size))
        columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

    size_t current_block_size = std::min(max_block_size, keys.size());
    as_batch batch;
    as_batch_inita(&batch, current_block_size);

    TemporaryName recordsHandler(&columns, description, cursor);
    for (UInt32 i = 0; i < current_block_size; ++i)
    {
        InitializeBatchKey(as_batch_keyat(&batch, i), "test", "test_set", keys[cursor + i]);
    }

    const auto batchReadCallback = [](const as_batch_read * results, uint32_t size, void * records_handler_) {
        TemporaryName * records_handler = static_cast<TemporaryName *>(records_handler_);
        uint32_t n_found = 0;

        for (uint32_t i = 0; i < size; i++)
        {
            if (results[i].result == AEROSPIKE_OK)
            {
                records_handler->HandleRecordBins(results[i].record);
                fprintf(stderr, "  AEROSPIKE_OK");
                n_found++;
            }
            else if (results[i].result == AEROSPIKE_ERR_RECORD_NOT_FOUND)
            {
                // The transaction succeeded but the record doesn't exist.
                fprintf(stderr, "  AEROSPIKE_ERR_RECORD_NOT_FOUND");
            }
            else
            {
                // The transaction didn't succeed.
                fprintf(stderr, "  error %d", results[i].result);
            }
        }
        return true;
    };


    as_error err;
    if (aerospike_batch_get(&client, &err, nullptr, &batch, batchReadCallback, static_cast<void *>(&recordsHandler)) != AEROSPIKE_OK)
    {
        fprintf(stderr, "aerospike_batch_get() returned %d - %s", err.code, err.message);
        exit(-1);
    }
    recordsHandler.HandleKeys(keys);

    size_t num_rows = recordsHandler.getNumRows();
    cursor += num_rows;

    assert(cursor <= keys.size());

    if (cursor == keys.size())
    {
        all_read = true;
    }

    if (num_rows == 0)
        return {};

    return description.sample_block.cloneWithColumns(std::move(columns));
}

}

#endif
