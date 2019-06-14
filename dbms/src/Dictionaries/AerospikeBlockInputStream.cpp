#include <Common/config.h>

#if USE_AEROSPIKE

#include <sstream>
#include <string>
#include <vector>
#include <ext/range.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitors.h>

#include "Aerospike.h"
#include "AerospikeBlockInputStream.h"
#include "DictionaryStructure.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int EXTERNAL_DICTIONARY_ERROR;
}

namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    class RecordsHandler
    {
    public:
        RecordsHandler(MutableColumns * columns, const ExternalResultDescription & description)
            : columns(columns), description(description)
        {
        }

        void HandleRecordBins(const as_record & record)
        {
            ++num_rows;
            for (const auto idx : ext::range(1, columns->size()))
            {
                const auto & name = description.sample_block.getByPosition(idx).name;

                const as_bin_value & bin_value = record.bins.entries[idx - 1].value;
                if (as_val_type(const_cast<as_bin_value*>(&bin_value)) == AS_NIL)
                {
                    insertDefaultValue(*(*columns)[idx], *description.sample_block.getByPosition(idx).column);
                }
                else
                {
                    if (description.types[idx].second)
                    {
                        ColumnNullable & column_nullable = static_cast<ColumnNullable &>(*(*columns)[idx]); // Use reference here because pointers cannot be casted
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
                    static_cast<ColumnVector<UInt8> &>(column).insertValue(key->value.integer.value);
                    break;
                case ValueType::UInt16:
                    static_cast<ColumnVector<UInt16> &>(column).insertValue(key->value.integer.value);
                    break;
                case ValueType::UInt32:
                    static_cast<ColumnVector<UInt32> &>(column).insertValue(key->value.integer.value);
                    break;
                case ValueType::UInt64:
                    static_cast<ColumnVector<UInt64> &>(column).insertValue(key->value.integer.value);
                    break;
                case ValueType::Int8:
                    static_cast<ColumnVector<Int8> &>(column).insertValue(key->value.integer.value);
                    break;
                case ValueType::Int16:
                    static_cast<ColumnVector<Int16> &>(column).insertValue(key->value.integer.value);
                    break;
                case ValueType::Int32:
                    static_cast<ColumnVector<Int32> &>(column).insertValue(key->value.integer.value);
                    break;
                case ValueType::Int64:
                    static_cast<ColumnVector<Int64> &>(column).insertValue(key->value.integer.value);
                    break;
                case ValueType::String: {
                    String str{key->value.string.value, key->value.string.len};
                    static_cast<ColumnString &>(column).insertDataWithTerminatingZero(str.data(), str.size() + 1);
                    break;
                }
                case ValueType::Date:
                    static_cast<ColumnUInt16 &>(column).insertValue(parse<LocalDate>(String(key->value.string.value, key->value.string.len)).getDayNum());
                    break;
                case ValueType::DateTime:
                    static_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(parse<LocalDateTime>(String(key->value.string.value, key->value.string.len))));
                    break;
                case ValueType::UUID:
                    static_cast<ColumnUInt128 &>(column).insertValue(parse<UUID>(String(key->value.string.value, key->value.string.len)));
                    break;
                default:
                    std::string invalid_type = toString(static_cast<int>(as_val_type(const_cast<as_key_value*>(&key->value))));
                    throw Exception{"Type mismatch, got type id = " + invalid_type + " for column " + name,
                                    ErrorCodes::TYPE_MISMATCH};
            }
        }

        template <typename T>
        void insertNumberValue(IColumn & column, const as_bin_value * value, const std::string & name)
        {
            switch (as_val_type(const_cast<as_bin_value*>(value)))
            {
                case AS_INTEGER:
                    static_cast<ColumnVector<T> &>(column).getData().push_back(value->integer.value);
                    break;
                case AS_DOUBLE:
                    static_cast<ColumnVector<T> &>(column).getData().push_back(value->dbl.value);
                    break;
                default:
                    std::string type = toString(static_cast<int>(as_val_type(const_cast<as_bin_value*>(value))));
                    throw Exception(
                        "Type mismatch, expected a number, got type id = " + type + " for column " + name, ErrorCodes::TYPE_MISMATCH);
            }
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
                case ValueType::String: {
                    String str{value->string.value, value->string.len};
                    static_cast<ColumnString &>(column).insertDataWithTerminatingZero(str.data(), str.size() + 1);
                    break;
                }
                case ValueType::Date:
                    static_cast<ColumnUInt16 &>(column).insertValue(parse<LocalDate>(String(value->string.value, value->string.len)).getDayNum());
                    break;
                case ValueType::DateTime:
                    static_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(parse<LocalDateTime>(String(value->string.value, value->string.len))));
                    break;
                case ValueType::UUID:
                    static_cast<ColumnUInt128 &>(column).insertValue(parse<UUID>(String(value->string.value, value->string.len)));
                    break;
            }
        }

        void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }

        MutableColumns * columns;
        const ExternalResultDescription & description;
        size_t num_rows = 0;
    };
}


AerospikeBlockInputStream::AerospikeBlockInputStream(
    Aerospike & client,
    std::vector<AerospikeKey> && keys,
    const Block & sample_block,
    const size_t max_block_size,
    const std::string & namespace_name,
    const std::string & set_name)
    : client(client)
    , keys(std::move(keys))
    , max_block_size{max_block_size}
    , namespace_name{namespace_name}
    , set_name{set_name}
{
    description.init(sample_block);
}

AerospikeBlockInputStream::~AerospikeBlockInputStream() = default;

Block AerospikeBlockInputStream::readImpl()
{
    const size_t size = description.sample_block.columns();
    if (all_read || size == 0)
        return {};

    MutableColumns columns(size);
    for (const auto i : ext::range(0, size))
        columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

    const auto batch_read_callback = [](const as_batch_read * results, uint32_t result_size, void * udata)
    {
        RecordsHandler * records_handler = static_cast<RecordsHandler *>(udata);

        for (uint32_t i = 0; i < result_size; i++)
        {
            if (results[i].result != AEROSPIKE_OK)
            {
                /// The transaction succeeded but the record doesn't exist.
                if (results[i].result == AEROSPIKE_ERR_RECORD_NOT_FOUND)
                    throw Exception{"Aerospike: record not found", ErrorCodes::EXTERNAL_DICTIONARY_ERROR};

                throw Exception{"Aerospike: read transaction fails", ErrorCodes::EXTERNAL_DICTIONARY_ERROR};
            }

            records_handler->HandleRecordBins(results[i].record);
        }
        return true;
    };

    size_t current_block_size = std::min(max_block_size, keys.size());
    as_batch batch;
    as_batch_inita(&batch, current_block_size);

    RecordsHandler records_handler(&columns, description);
    for (UInt32 i = 0; i < current_block_size; ++i)
        InitializeBatchKey(as_batch_keyat(&batch, i), namespace_name.c_str(), set_name.c_str(), keys[cursor + i]);

    if (!client.batchGet(&batch, batch_read_callback, &records_handler))
        throw Exception{client.lastErrorMessage(), ErrorCodes::EXTERNAL_DICTIONARY_ERROR};

    records_handler.HandleKeys(keys);

    size_t num_rows = records_handler.getNumRows();
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
