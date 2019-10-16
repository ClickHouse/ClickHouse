// #include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

// namespace DB
// {

// class MergeTreeDataPartWriterWide : IMergeTreeDataPartWriter
// {
// public:
//     size_t write(size_t current_mark, const Block & block) override;

//     std::pair<size_t, size_t> writeColumn(
//         const String & name,
//         const IDataType & type,
//         const IColumn & column,
//         WrittenOffsetColumns & offset_columns,
//         bool skip_offsets,
//         IDataType::SerializeBinaryBulkStatePtr & serialization_state,
//         size_t from_mark) override;

// protected:
//     void start() override;

// private:
//     SerializationStates serialization_states;
//     NameSet permuted_columns;
// };

// }