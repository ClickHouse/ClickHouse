#    include <Common/config.h>
#if USE_AEROSPIKE

#    include <sstream>
#    include <string>
#    include <vector>

#    include <aerospike/aerospike.h>
#    include <aerospike/as_batch.h>
#    include <aerospike/aerospike_batch.h>
#    include <aerospike/as_record.h>
#    include <aerospike/as_scan.h>
#    include <aerospike/as_val.h>
#    include <aerospike/aerospike_scan.h>

#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnsNumber.h>
#    include <IO/ReadHelpers.h>
#    include <IO/WriteHelpers.h>
#    include <Common/FieldVisitors.h>
#    include <ext/range.h>
#    include "DictionaryStructure.h"
#    include "AerospikeBlockInputStream.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

AerospikeBlockInputStream::AerospikeBlockInputStream(
    const aerospike & client,
    std::vector<as_key> keys,
    const Block & sample_block,
    const size_t max_block_size)
    : client(client)
    , keys(std::move(keys))
    , max_block_size{max_block_size}
{
    description.init(sample_block);
}

AerospikeBlockInputStream::~AerospikeBlockInputStream() = default;

namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    class TemporaryName {
    public:
        TemporaryName(MutableColumns * columns, const ExternalResultDescription& description, size_t cursor)
        : columns(columns)
        , description(description)
        , cursor(cursor)
        {}

        void HandleRecord(const as_record& record) {

            ++cursor;
            const auto & name = description.sample_block.getByPosition(0).name;
            insertKey(*((*columns)[0]), description.types[0].first, as_rec_key(&record._), &record, name); // TODO(gleb777) handle null result

            /*for (const auto idx : ext::range(0, columns->size()))
            {
                const auto & name = description.sample_block.getByPosition(idx).name;

                if (value.isNull() || value->type() == Poco::MongoDB::ElementTraits<Poco::MongoDB::NullValue>::TypeId)
                    insertDefaultValue(*columns[idx], *description.sample_block.getByPosition(idx).column);
                else
                {
                    if (description.types[idx].second)
                    {
                        ColumnNullable & column_nullable = static_cast<ColumnNullable &>(*columns[idx]);
                        insertValue(column_nullable.getNestedColumn(), description.types[idx].first, *value, name);
                        column_nullable.getNullMapData().emplace_back(0);
                    }
                    else
                        insertValue(*columns[idx], description.types[idx].first, *value, name);
                }
            }*/
        }

        size_t getNumRows() const {
            return num_rows;
        }
    private:

        void insertKey(IColumn& column, const ValueType type, const as_val* value, const as_record* record, const std::string& name) {
            switch (type) {
                case ValueType::UInt8:
                    static_cast<ColumnVector<UInt8>&>(column).getData().push_back(record->key.value.integer.value);
                    break;
                case ValueType::UInt16:
                    static_cast<ColumnVector<UInt16>&>(column).getData().push_back(record->key.value.integer.value);
                    break;
                case ValueType::UInt32:
                    static_cast<ColumnVector<UInt32>&>(column).getData().push_back(record->key.value.integer.value);
                    break;
                case ValueType::UInt64:
                    static_cast<ColumnVector<UInt64>&>(column).getData().push_back(record->key.value.integer.value);
                    break;
                case ValueType::Int8:
                    static_cast<ColumnVector<Int8>&>(column).getData().push_back(record->key.value.integer.value);
                    break;
                case ValueType::Int16:
                    static_cast<ColumnVector<Int16>&>(column).getData().push_back(record->key.value.integer.value);
                    break;
                case ValueType::Int32:
                    static_cast<ColumnVector<Int32>&>(column).getData().push_back(record->key.value.integer.value);
                    break;
                case ValueType::Int64:
                    static_cast<ColumnVector<Int64>&>(column).getData().push_back(record->key.value.integer.value);
                    break;
                case ValueType::String: {
                    String str{record->key.value.string.value, record->key.value.string.len};
                    static_cast<ColumnString&>(column).insertDataWithTerminatingZero(str.data(), str.size() + 1);
                    break;
                }
                case ValueType::Date:
                    static_cast<ColumnUInt16&>(column).getData().push_back(UInt16{DateLUT::instance().toDayNum(
                        static_cast<Int64>(record->key.value.integer.value))});
                    break;
                case ValueType::DateTime:
                    static_cast<ColumnUInt32&>(column).getData().push_back(
                        static_cast<Int64>(record->key.value.integer.value));
                    break;
                case ValueType::UUID: {
                    String str{record->key.value.string.value, record->key.value.string.len};
                    static_cast<ColumnUInt128&>(column).getData().push_back(parse<UUID>(str));
                    break;
                }
                default:
                    throw Exception{
                        "Type mismatch, expected String (UUID), got type id = " + toString(value->type) +
                        " for column "
                        + name,
                        ErrorCodes::TYPE_MISMATCH};
            }
        }

        template <typename T>
        void insertNumberValue(IColumn & column, const as_record * record, const std::string & name) {
            (void)name;
            switch(as_val_type(&(record->bins.entries[0].value))) {
                case AS_INTEGER:
                    static_cast<ColumnVector<T>&>(column).getData().push_back(record->bins.entries[0].value.integer.value);
                    break;
                case AS_DOUBLE:
                    static_cast<ColumnVector<T>&>(column).getData().push_back(record->bins.entries[0].value.dbl.value);
                    break;
                /*case AS_STRING:
                    static_cast<ColumnVector<T>&>(column).getData().push_back(record->bins.entries[0].value.string.value);
                    break; NOBODY USE IT */
                default:
                    std::string type = "bad"; // toString(as_val_type(&(record->bins.entries[0].value))); // TODO:FIX_ME(glebx777)
                    throw Exception(
                        "Type mismatch, expected a number, got type id = " + type + " for column " + name,
                        ErrorCodes::TYPE_MISMATCH);
            }

        }

        void insertValue(IColumn& column, const ValueType type, const as_val* value, const as_record * record, const std::string& name) {
            switch (type) {
                case ValueType::UInt8:
                    insertNumberValue<UInt8>(column, record, name);
                    break;
                case ValueType::UInt16:
                    insertNumberValue<UInt16>(column, record, name);
                    break;
                case ValueType::UInt32:
                    insertNumberValue<UInt32>(column, record, name);
                    break;
                case ValueType::UInt64:
                    insertNumberValue<UInt64>(column, record, name);
                    break;
                case ValueType::Int8:
                    insertNumberValue<Int8>(column, record, name);
                    break;
                case ValueType::Int16:
                    insertNumberValue<Int16>(column, record, name);
                    break;
                case ValueType::Int32:
                    insertNumberValue<Int32>(column, record, name);
                    break;
                case ValueType::Int64:
                    insertNumberValue<Int64>(column, record, name);
                    break;
                case ValueType::String: {
                    String str{record->bins.entries[0].value.string.value, record->bins.entries[0].value.string.len};
                    static_cast<ColumnString&>(column).insertDataWithTerminatingZero(str.data(), str.size() + 1);
                    break;
                }
                case ValueType::Date:
                    static_cast<ColumnUInt16&>(column).getData().push_back(UInt16{DateLUT::instance().toDayNum(
                        static_cast<Int64>(record->bins.entries[0].value.integer.value))});
                    break;
                case ValueType::DateTime:
                    static_cast<ColumnUInt32&>(column).getData().push_back(
                        static_cast<Int64>(record->bins.entries[0].value.integer.value));
                    break;
                case ValueType::UUID: {
                    String str{record->bins.entries[0].value.string.value, record->bins.entries[0].value.string.len};
                    static_cast<ColumnUInt128&>(column).getData().push_back(parse<UUID>(str));
                    break;
                }
                default:
                    throw Exception{
                        "Type mismatch, expected String (UUID), got type id = " + toString(value->type) +
                        " for column "
                        + name,
                        ErrorCodes::TYPE_MISMATCH};
            }
        }

        void insertDefaultValue(IColumn& column, const IColumn& sample_column) {
            column.insertFrom(sample_column, 0);
        }

        /*template <typename T>
        void insertValueByIdx(size_t idx, const T & value) {
            const auto & name = description.sample_block.getByPosition(idx).name;
            if (description.types[idx].second)
            {
                ColumnNullable & column_nullable = static_cast<ColumnNullable &>(*columns[idx]);
                insertValue(column_nullable.getNestedColumn(), description.types[idx].first, value, name);
                column_nullable.getNullMapData().emplace_back(0);
            }
            else
                insertValue(*(*columns)[idx], description.types[idx].first, value, name);
        }*/

        MutableColumns * columns;
        const ExternalResultDescription & description;
        size_t num_rows = 0;
        size_t cursor = 0;
    };
}


Block AerospikeBlockInputStream::readImpl()
{
    if (all_read)
        return {};

    MutableColumns columns(description.sample_block.columns());
   // const size_t size = columns.size();

    const size_t size = 2;
    assert(size == description.sample_block.columns());


    for (const auto i : ext::range(0, size))
        columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

    size_t current_block_size = std::min(max_block_size, keys.size());
    as_batch batch;
    as_batch_inita(&batch, current_block_size);

    TemporaryName recordsHandler(&columns, description, cursor);
    for (UInt32 i = 0; i < current_block_size; ++i) {
        fprintf(stderr, "KEY VALUE: %s \n", keys[cursor + i].value.string.value);
        as_key_init_value(as_batch_keyat(&batch, i), "test", "test_set", (as_key_value*)(&(keys[cursor + i].value)));
        fprintf(stderr, "KEY VALUE: %s \n", as_batch_keyat(&batch, i)->value.string.value);
    }

    const auto batchReadCallback = [] (const as_batch_read* results, uint32_t size, void* records_handler_) {
        TemporaryName* records_handler = static_cast<TemporaryName*>(records_handler_);
        uint32_t n_found = 0;

        for (uint32_t i = 0; i < size; i++) {
            if (results[i].result == AEROSPIKE_OK) {
                records_handler->HandleRecord(results[i].record);
                printf("  AEROSPIKE_OK");
                n_found++;
            }
            else if (results[i].result == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
                // The transaction succeeded but the record doesn't exist.
                printf("  AEROSPIKE_ERR_RECORD_NOT_FOUND");
            }
            else {
                // The transaction didn't succeed.
                printf("  error %d", results[i].result);
            }
        }

        return true;
    };


    as_error err;
    if (aerospike_batch_get(&client, &err, nullptr, &batch, batchReadCallback, static_cast<void*>(&recordsHandler)) != AEROSPIKE_OK) {
        printf("aerospike_batch_get() returned %d - %s", err.code, err.message);
        exit(-1);
    }

    size_t num_rows = recordsHandler.getNumRows();
    cursor += num_rows;

    assert(cursor <= keys.size());

    if (cursor == keys.size()) {
        all_read = true;
    }

    if (num_rows == 0)
        return {};

    return description.sample_block.cloneWithColumns(std::move(columns));
}

}

#endif
