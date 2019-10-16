// #include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>

// namespace DB
// {

// size_t MergeTreeDataPartWriterWide::write(size_t current_mark, const Block & block)
// {
//     if (!started)
//         start();

//     size_t index_offset = 0;
//     auto it = columns_list.begin();
//     for (size_t i = 0; i < columns_list.size(); ++i, ++it)
//     {
//         const ColumnWithTypeAndName & column = block.getByName(it->name);

//         if (permutation)
//         {
//             auto primary_column_it = primary_key_column_name_to_position.find(it->name);
//             auto skip_index_column_it = skip_indexes_column_name_to_position.find(it->name);

//             if (primary_key_column_name_to_position.end() != primary_column_it)
//             {
//                 const auto & primary_column = *primary_key_columns[primary_column_it->second].column;
//                 std::tie(std::ignore, index_offset) = writeColumn(column.name, *column.type, primary_column, offset_columns, false, serialization_states[i], current_mark);
//             }
//             else if (skip_indexes_column_name_to_position.end() != skip_index_column_it)
//             {
//                 const auto & index_column = *skip_indexes_columns[skip_index_column_it->second].column;
//                 std::tie(std::ignore, index_offset) = writeColumn(column.name, *column.type, index_column, offset_columns, false, serialization_states[i], current_mark);
//             }
//             else
//             {
//                 /// We rearrange the columns that are not included in the primary key here; Then the result is released - to save RAM.
//                 ColumnPtr permuted_column = column.column->permute(*permutation, 0);
//                 std::tie(std::ignore, index_offset) = writeColumn(column.name, *column.type, *permuted_column, offset_columns, false, serialization_states[i], current_mark);
//             }
//         }
//         else
//         {
//             std::tie(std::ignore, index_offset) = writeColumn(column.name, *column.type, *column.column, offset_columns, false, serialization_states[i], current_mark);
//         }
//     }

//     return index_offset;
// }

// void MergeTreeDataPartWriterWide::start()
// {
//     if (started)
//         return;

//     started = true;

//     serialization_states.reserve(columns_list.size());
//     WrittenOffsetColumns tmp_offset_columns;
//     IDataType::SerializeBinaryBulkSettings settings;

//     for (const auto & col : columns_list)
//     {
//         settings.getter = createStreamGetter(col.name, tmp_offset_columns, false);
//         serialization_states.emplace_back(nullptr);
//         col.type->serializeBinaryBulkStatePrefix(settings, serialization_states.back());
//     }
// }

// }
