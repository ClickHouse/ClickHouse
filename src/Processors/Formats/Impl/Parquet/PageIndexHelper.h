#pragma once

#include <base/types.h>
#include <DataTypes/IDataType.h>
#include <Processors/Formats/Impl/Parquet/ColumnFilter.h>

namespace DB
{

class PageIndexHelper
{
public:
    // Helper function to handle conversion for numeric types
    template <typename T>
    static std::pair<T, T> convertNumeric(const String &min_value, const String &max_value)
    {
        chassert(min_value.size() == sizeof(T) && max_value.size() == sizeof(T));
        T min = *reinterpret_cast<const T *>(min_value.data());
        T max = *reinterpret_cast<const T *>(max_value.data());
        return std::make_pair(min, max);
    }

    template <typename T>
    std::pair<T, T> convertMinMaxValue(const String min_value, const String max_value, TypeIndex type) const
    {
        switch (type){
            case TypeIndex::UInt8: {
                return convertNumeric<UInt8>(min_value, max_value);
            }
            case TypeIndex::UInt16: {
                return convertNumeric<UInt16>(min_value, max_value);
            }
            case TypeIndex::UInt32: {
                return convertNumeric<UInt32>(min_value, max_value);
            }
            case TypeIndex::UInt64: {
                return convertNumeric<UInt64>(min_value, max_value);
            }
            case TypeIndex::Int8: {
                return convertNumeric<Int8>(min_value, max_value);
            }
            case TypeIndex::Int16: {
                return convertNumeric<Int16>(min_value, max_value);
            }
            case TypeIndex::Int32: {
                return convertNumeric<Int32>(min_value, max_value);
            }
            case TypeIndex::Int64: {
                return convertNumeric<Int64>(min_value, max_value);
            }
            case TypeIndex::Float32: {
                return convertNumeric<Float32>(min_value, max_value);
            }
            case TypeIndex::Float64: {
                return convertNumeric<Float64>(min_value, max_value);
            }
            case TypeIndex::Date32: {
                return convertNumeric<Int32>(min_value, max_value);
            }
            case TypeIndex::String:
                return std::make_pair(min_value, max_value);
            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported type");
        }
    }
};
}
