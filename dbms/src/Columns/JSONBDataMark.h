#pragma once

//#include <Columns/ColumnsNumber.h>
//#include <Columns/ColumnJSONB.h>

namespace DB
{

enum class JSONBDataMark
{
    NothingMark = 0,
    BoolMark, Int64Mark, UInt64Mark, Float64Mark,
    StringMark, ObjectMark, ArrayMark, NullMark,
    End = 32,
};

//struct JSONBDataMarkType
//{
////    const static DataTypePtr & BINARY_JSON_TYPE = std::make_shared<DataTypeString>();
////    const static DataTypePtr & NUMBER_DATA_TYPE = std::make_shared<DataTypeUInt64>();
////    const static DataTypePtr & STRING_DATA_TYPE = std::make_shared<DataTypeString>();
////    const static DataTypePtr & BOOLEAN_DATA_TYPE = std::make_shared<DataTypeUInt8>();
//
//    template <JSONBDataMark data_mark, typename ColumnType>
//    static inline ColumnType * getDataMarkType(const ColumnJSONBStructPtr & column_struct)
//    {
//        if constexpr (std::is_same_v<ColumnType, ColumnUInt8> && data_mark == JSONBDataMark::Bool)
//            return static_cast<ColumnType *>(column_struct->getDataColumn(BINARY_JSON_TYPE));
//        else if constexpr (std::is_same_v<ColumnType, ColumnInt64> && data_mark == JSONBDataMark::Int64)
//            return static_cast<ColumnType *>(column_struct->getDataColumn(BINARY_JSON_TYPE));
//    }
//};

}


