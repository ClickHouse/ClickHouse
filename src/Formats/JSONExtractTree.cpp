//#include <Formats/JSONExtractTree.h>
//
//#include <Common/JSONParsers/DummyJSONParser.h>
//#include <Common/JSONParsers/SimdJSONParser.h>
//#include <Common/JSONParsers/RapidJSONParser.h>
//#include <Core/AccurateComparison.h>
//
//#include <Columns/ColumnVector.h>
//#include <Columns/ColumnString.h>
//#include <Columns/ColumnLowCardinality.h>
//
//#include <IO/ReadBufferFromMemory.h>
//#include <IO/ReadHelpers.h>
//#include <IO/WriteHelpers.h>
//
//namespace DB
//{
//
//namespace
//{
//
//const FormatSettings & getFormatSettings()
//{
//    static const FormatSettings instance = []
//    {
//        FormatSettings settings;
//        settings.json.escape_forward_slashes = false;
//        return settings;
//    }();
//    return instance;
//}
//
//template <typename Element>
//void elementToString(const Element & element, WriteBuffer & buf)
//{
//    if (element.isInt64())
//    {
//        writeIntText(element.getInt64(), buf);
//        return;
//    }
//    if (element.isUInt64())
//    {
//        writeIntText(element.getUInt64(), buf);
//        return;
//    }
//    if (element.isDouble())
//    {
//        writeFloatText(element.getDouble(), buf);
//        return;
//    }
//    if (element.isBool())
//    {
//        if (element.getBool())
//            writeCString("true", buf);
//        else
//            writeCString("false", buf);
//        return;
//    }
//    if (element.isString())
//    {
//        writeJSONString(element.getString(), buf, getFormatSettings());
//        return;
//    }
//    if (element.isArray())
//    {
//        writeChar('[', buf);
//        bool need_comma = false;
//        for (auto value : element.getArray())
//        {
//            if (std::exchange(need_comma, true))
//                writeChar(',', buf);
//            elementToString(value, buf);
//        }
//        writeChar(']', buf);
//        return;
//    }
//    if (element.isObject())
//    {
//        writeChar('{', buf);
//        bool need_comma = false;
//        for (auto [key, value] : element.getObject())
//        {
//            if (std::exchange(need_comma, true))
//                writeChar(',', buf);
//            writeJSONString(key, buf, getFormatSettings());
//            writeChar(':', buf);
//            elementToString(value, buf);
//        }
//        writeChar('}', buf);
//        return;
//    }
//    if (element.isNull())
//    {
//        writeCString("null", buf);
//        return;
//    }
//}
//
//template <typename Element, typename NumberType>
//class NumericNode : public JSONExtractTree<Element>::Node
//{
//public:
//    NumericNode(bool convert_bool_to_integer_) : convert_bool_to_integer(convert_bool_to_integer_) {}
//
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        NumberType value;
//        if (!tryGetValue(element, value))
//            return false;
//
//        auto & col_vec = assert_cast<ColumnVector<NumberType> &>(dest);
//        col_vec.insertValue(value);
//        return true;
//    }
//
//    bool tryGetValue(const Element & element, NumberType & value)
//    {
//        switch (element.type())
//        {
//            case ElementType::DOUBLE:
//                if constexpr (std::is_floating_point_v<NumberType>)
//                {
//                    /// We permit inaccurate conversion of double to float.
//                    /// Example: double 0.1 from JSON is not representable in float.
//                    /// But it will be more convenient for user to perform conversion.
//                    value = static_cast<NumberType>(element.getDouble());
//                }
//                else if (!accurate::convertNumeric<Float64, NumberType, false>(element.getDouble(), value))
//                    return false;
//                break;
//            case ElementType::UINT64:
//                if (!accurate::convertNumeric<UInt64, NumberType, false>(element.getUInt64(), value))
//                    return false;
//                break;
//            case ElementType::INT64:
//                if (!accurate::convertNumeric<Int64, NumberType, false>(element.getInt64(), value))
//                    return false;
//                break;
//            case ElementType::BOOL:
//                if constexpr (is_integer<NumberType>)
//                {
//                    if (convert_bool_to_integer)
//                    {
//                        value = static_cast<NumberType>(element.getBool());
//                        break;
//                    }
//                }
//                return false;
//            case ElementType::STRING:
//            {
//                auto rb = ReadBufferFromMemory{element.getString()};
//                if constexpr (std::is_floating_point_v<NumberType>)
//                {
//                    if (!tryReadFloatText(value, rb) || !rb.eof())
//                        return false;
//                }
//                else
//                {
//                    if (tryReadIntText(value, rb) && rb.eof())
//                        break;
//
//                    /// Try to parse float and convert it to integer.
//                    Float64 tmp_float;
//                    rb.position() = rb.buffer().begin();
//                    if (!tryReadFloatText(tmp_float, rb) || !rb.eof())
//                        return false;
//
//                    if (!accurate::convertNumeric<Float64, NumberType, false>(tmp_float, value))
//                        return false;
//                }
//                break;
//            }
//            case ElementType::NULL_VALUE:
//            {
//                if ()
//            }
//            default:
//                return false;
//        }
//
//        return true;
//    }
//
//private:
//    bool convert_bool_to_integer;
//};
//
//template <typename Element, typename NumberType>
//class LowCardinalityNumericNode : public NumericNode<Element, NumberType>
//{
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        NumberType value;
//        if (!tryGetValue(element, value))
//            return false;
//
//        auto & col_lc = assert_cast<ColumnLowCardinality &>(dest);
//        col_lc.insertData(reinterpret_cast<const char *>(&value), sizeof(value));
//        return true;
//    }
//};
//
//template <typename Element>
//class StringNode : public JSONExtractTree<Element>::Node
//{
//public:
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        if (element.isNull())
//            return false;
//
//        if (!element.isString())
//        {
//            ColumnString & col_str = assert_cast<ColumnString &>(dest);
//            auto & chars = col_str.getChars();
//            WriteBufferFromVector<ColumnString::Chars> buf(chars, AppendModeTag());
//            elementToString(element, buf);
//            buf.finalize();
//            chars.push_back(0);
//            col_str.getOffsets().push_back(chars.size());
//            return true;
//        }
//        else
//        {
//            auto str = element.getString();
//            ColumnString & col_str = assert_cast<ColumnString &>(dest);
//            col_str.insertData(str.data(), str.size());
//        }
//        return true;
//    }
//};
//
//template <typename Element>
//class LowCardinalityStringNode : public JSONExtractTree<Element>::Node
//{
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        if (element.isNull())
//            return false;
//
//        if (!element.isString())
//        {
//            ColumnString & col_str = assert_cast<ColumnString &>(dest);
//            auto & chars = col_str.getChars();
//            WriteBufferFromVector<ColumnString::Chars> buf(chars, AppendModeTag());
//            elementToString(element, buf);
//            buf.finalize();
//            chars.push_back(0);
//            col_str.getOffsets().push_back(chars.size());
//            return true;
//        }
//        else
//        {
//            auto str = element.getString();
//            ColumnString & col_str = assert_cast<ColumnString &>(dest);
//            col_str.insertData(str.data(), str.size());
//        }
//        return true;
//    }
//};
//
//
//
//
//
//
//class LowCardinalityFixedStringNode : public Node
//{
//public:
//    explicit LowCardinalityFixedStringNode(const size_t fixed_length_) : fixed_length(fixed_length_) { }
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        // If element is an object we delegate the insertion to JSONExtractRawImpl
//        if (element.isObject())
//            return JSONExtractRawImpl<JSONParser>::insertResultToLowCardinalityFixedStringColumn(dest, element, fixed_length);
//        else if (!element.isString())
//            return false;
//
//        auto str = element.getString();
//        if (str.size() > fixed_length)
//            return false;
//
//        // For the non low cardinality case of FixedString, the padding is done in the FixedString Column implementation.
//        // In order to avoid having to pass the data to a FixedString Column and read it back (which would slow down the execution)
//        // the data is padded here and written directly to the Low Cardinality Column
//        if (str.size() == fixed_length)
//        {
//            assert_cast<ColumnLowCardinality &>(dest).insertData(str.data(), str.size());
//        }
//        else
//        {
//            String padded_str(str);
//            padded_str.resize(fixed_length, '\0');
//
//            assert_cast<ColumnLowCardinality &>(dest).insertData(padded_str.data(), padded_str.size());
//        }
//        return true;
//    }
//
//private:
//    const size_t fixed_length;
//};
//
//class UUIDNode : public Node
//{
//public:
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        if (!element.isString())
//            return false;
//
//        auto uuid = parseFromString<UUID>(element.getString());
//        if (dest.getDataType() == TypeIndex::LowCardinality)
//        {
//            ColumnLowCardinality & col_low = assert_cast<ColumnLowCardinality &>(dest);
//            col_low.insertData(reinterpret_cast<const char *>(&uuid), sizeof(uuid));
//        }
//        else
//        {
//            assert_cast<ColumnUUID &>(dest).insert(uuid);
//        }
//        return true;
//    }
//};
//
//template <typename DecimalType>
//class DecimalNode : public Node
//{
//public:
//    explicit DecimalNode(DataTypePtr data_type_) : data_type(data_type_) {}
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        const auto * type = assert_cast<const DataTypeDecimal<DecimalType> *>(data_type.get());
//
//        DecimalType value{};
//
//        switch (element.type())
//        {
//            case ElementType::DOUBLE:
//                value = convertToDecimal<DataTypeNumber<Float64>, DataTypeDecimal<DecimalType>>(
//                    element.getDouble(), type->getScale());
//                break;
//            case ElementType::UINT64:
//                value = convertToDecimal<DataTypeNumber<UInt64>, DataTypeDecimal<DecimalType>>(
//                    element.getUInt64(), type->getScale());
//                break;
//            case ElementType::INT64:
//                value = convertToDecimal<DataTypeNumber<Int64>, DataTypeDecimal<DecimalType>>(
//                    element.getInt64(), type->getScale());
//                break;
//            case ElementType::STRING: {
//                auto rb = ReadBufferFromMemory{element.getString()};
//                if (!SerializationDecimal<DecimalType>::tryReadText(value, rb, DecimalUtils::max_precision<DecimalType>, type->getScale()))
//                    return false;
//                break;
//            }
//            default:
//                return false;
//        }
//
//        assert_cast<ColumnDecimal<DecimalType> &>(dest).insertValue(value);
//        return true;
//    }
//
//private:
//    DataTypePtr data_type;
//};
//
//class FixedStringNode : public Node
//{
//public:
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        if (element.isNull())
//            return false;
//
//        if (!element.isString())
//            return JSONExtractRawImpl<JSONParser>::insertResultToFixedStringColumn(dest, element, {});
//
//        auto str = element.getString();
//        auto & col_str = assert_cast<ColumnFixedString &>(dest);
//        if (str.size() > col_str.getN())
//            return false;
//        col_str.insertData(str.data(), str.size());
//
//        return true;
//    }
//};
//
//template <typename Type>
//class EnumNode : public Node
//{
//public:
//    explicit EnumNode(const std::vector<std::pair<String, Type>> & name_value_pairs_) : name_value_pairs(name_value_pairs_)
//    {
//        for (const auto & name_value_pair : name_value_pairs)
//        {
//            name_to_value_map.emplace(name_value_pair.first, name_value_pair.second);
//            only_values.emplace(name_value_pair.second);
//        }
//    }
//
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        auto & col_vec = assert_cast<ColumnVector<Type> &>(dest);
//
//        if (element.isInt64())
//        {
//            Type value;
//            if (!accurate::convertNumeric(element.getInt64(), value) || !only_values.contains(value))
//                return false;
//            col_vec.insertValue(value);
//            return true;
//        }
//
//        if (element.isUInt64())
//        {
//            Type value;
//            if (!accurate::convertNumeric(element.getUInt64(), value) || !only_values.contains(value))
//                return false;
//            col_vec.insertValue(value);
//            return true;
//        }
//
//        if (element.isString())
//        {
//            auto value = name_to_value_map.find(element.getString());
//            if (value == name_to_value_map.end())
//                return false;
//            col_vec.insertValue(value->second);
//            return true;
//        }
//
//        return false;
//    }
//
//private:
//    std::vector<std::pair<String, Type>> name_value_pairs;
//    std::unordered_map<std::string_view, Type> name_to_value_map;
//    std::unordered_set<Type> only_values;
//};
//
//class NullableNode : public Node
//{
//public:
//    explicit NullableNode(std::unique_ptr<Node> nested_) : nested(std::move(nested_)) {}
//
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        if (dest.getDataType() == TypeIndex::LowCardinality)
//        {
//            /// We do not need to handle nullability in that case
//            /// because nested node handles LowCardinality columns and will call proper overload of `insertData`
//            return nested->insertResultToColumn(dest, element);
//        }
//
//        ColumnNullable & col_null = assert_cast<ColumnNullable &>(dest);
//        if (!nested->insertResultToColumn(col_null.getNestedColumn(), element))
//            return false;
//        col_null.getNullMapColumn().insertValue(0);
//        return true;
//    }
//
//private:
//    std::unique_ptr<Node> nested;
//};
//
//class ArrayNode : public Node
//{
//public:
//    explicit ArrayNode(std::unique_ptr<Node> nested_) : nested(std::move(nested_)) {}
//
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        if (!element.isArray())
//            return false;
//
//        auto array = element.getArray();
//
//        ColumnArray & col_arr = assert_cast<ColumnArray &>(dest);
//        auto & data = col_arr.getData();
//        size_t old_size = data.size();
//        bool were_valid_elements = false;
//
//        for (auto value : array)
//        {
//            if (nested->insertResultToColumn(data, value))
//                were_valid_elements = true;
//            else
//                data.insertDefault();
//        }
//
//        if (!were_valid_elements)
//        {
//            data.popBack(data.size() - old_size);
//            return false;
//        }
//
//        col_arr.getOffsets().push_back(data.size());
//        return true;
//    }
//
//private:
//    std::unique_ptr<Node> nested;
//};
//
//class TupleNode : public Node
//{
//public:
//    TupleNode(std::vector<std::unique_ptr<Node>> nested_, const std::vector<String> & explicit_names_) : nested(std::move(nested_)), explicit_names(explicit_names_)
//    {
//        for (size_t i = 0; i != explicit_names.size(); ++i)
//            name_to_index_map.emplace(explicit_names[i], i);
//    }
//
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        ColumnTuple & tuple = assert_cast<ColumnTuple &>(dest);
//        size_t old_size = dest.size();
//        bool were_valid_elements = false;
//
//        auto set_size = [&](size_t size)
//        {
//            for (size_t i = 0; i != tuple.tupleSize(); ++i)
//            {
//                auto & col = tuple.getColumn(i);
//                if (col.size() != size)
//                {
//                    if (col.size() > size)
//                        col.popBack(col.size() - size);
//                    else
//                        while (col.size() < size)
//                            col.insertDefault();
//                }
//            }
//        };
//
//        if (element.isArray())
//        {
//            auto array = element.getArray();
//            auto it = array.begin();
//
//            for (size_t index = 0; (index != nested.size()) && (it != array.end()); ++index)
//            {
//                if (nested[index]->insertResultToColumn(tuple.getColumn(index), *it++))
//                    were_valid_elements = true;
//                else
//                    tuple.getColumn(index).insertDefault();
//            }
//
//            set_size(old_size + static_cast<size_t>(were_valid_elements));
//            return were_valid_elements;
//        }
//
//        if (element.isObject())
//        {
//            auto object = element.getObject();
//            if (name_to_index_map.empty())
//            {
//                auto it = object.begin();
//                for (size_t index = 0; (index != nested.size()) && (it != object.end()); ++index)
//                {
//                    if (nested[index]->insertResultToColumn(tuple.getColumn(index), (*it++).second))
//                        were_valid_elements = true;
//                    else
//                        tuple.getColumn(index).insertDefault();
//                }
//            }
//            else
//            {
//                for (const auto & [key, value] : object)
//                {
//                    auto index = name_to_index_map.find(key);
//                    if (index != name_to_index_map.end())
//                    {
//                        if (nested[index->second]->insertResultToColumn(tuple.getColumn(index->second), value))
//                            were_valid_elements = true;
//                    }
//                }
//            }
//
//            set_size(old_size + static_cast<size_t>(were_valid_elements));
//            return were_valid_elements;
//        }
//
//        return false;
//    }
//
//private:
//    std::vector<std::unique_ptr<Node>> nested;
//    std::vector<String> explicit_names;
//    std::unordered_map<std::string_view, size_t> name_to_index_map;
//};
//
//class MapNode : public Node
//{
//public:
//    MapNode(std::unique_ptr<Node> key_, std::unique_ptr<Node> value_) : key(std::move(key_)), value(std::move(value_)) { }
//
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        if (!element.isObject())
//            return false;
//
//        ColumnMap & map_col = assert_cast<ColumnMap &>(dest);
//        auto & offsets = map_col.getNestedColumn().getOffsets();
//        auto & tuple_col = map_col.getNestedData();
//        auto & key_col = tuple_col.getColumn(0);
//        auto & value_col = tuple_col.getColumn(1);
//        size_t old_size = tuple_col.size();
//
//        auto object = element.getObject();
//        auto it = object.begin();
//        for (; it != object.end(); ++it)
//        {
//            auto pair = *it;
//
//            /// Insert key
//            key_col.insertData(pair.first.data(), pair.first.size());
//
//            /// Insert value
//            if (!value->insertResultToColumn(value_col, pair.second))
//                value_col.insertDefault();
//        }
//
//        offsets.push_back(old_size + object.size());
//        return true;
//    }
//
//private:
//    std::unique_ptr<Node> key;
//    std::unique_ptr<Node> value;
//};
//
//class VariantNode : public Node
//{
//public:
//    VariantNode(std::vector<std::unique_ptr<Node>> variant_nodes_, std::vector<size_t> order_) : variant_nodes(std::move(variant_nodes_)), order(std::move(order_)) { }
//
//    bool insertResultToColumn(IColumn & dest, const Element & element) override
//    {
//        auto & column_variant = assert_cast<ColumnVariant &>(dest);
//        for (size_t i : order)
//        {
//            auto & variant = column_variant.getVariantByGlobalDiscriminator(i);
//            if (variant_nodes[i]->insertResultToColumn(variant, element))
//            {
//                column_variant.getLocalDiscriminators().push_back(column_variant.localDiscriminatorByGlobal(i));
//                column_variant.getOffsets().push_back(variant.size() - 1);
//                return true;
//            }
//        }
//
//        return false;
//    }
//
//private:
//    std::vector<std::unique_ptr<Node>> variant_nodes;
//    /// Order in which we should try variants nodes.
//    /// For example, String should be always the last one.
//    std::vector<size_t> order;
//};
//
//}
//
//}
