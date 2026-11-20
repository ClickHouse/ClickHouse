#include <Processors/Formats/Impl/ORC/Type.h>

#include <Common/Exception.h>
#include <Common/checkStackSize.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_TYPE;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::ORC
{

namespace
{

bool isUnquotedFieldName(const std::string & name)
{
    for (char c : name)
        if (!isalnum(static_cast<unsigned char>(c)) && c != '_')
            return false;
    return true;
}

std::string algorithmToString(UInt32 algorithm)
{
    switch (algorithm)
    {
        case 0: return "spherical";
        case 1: return "vincenty";
        case 2: return "thomas";
        case 3: return "andoyer";
        case 4: return "karney";
        default: return std::to_string(algorithm);
    }
}

/// The same limit as in the reference implementation.
constexpr size_t MAX_TYPE_NESTING_DEPTH = 2000;

std::unique_ptr<Type> convertType(const std::vector<Proto::Type> & types, UInt64 index, UInt64 & next_id, size_t depth)
{
    checkStackSize();

    if (depth > MAX_TYPE_NESTING_DEPTH)
        throw Exception(ErrorCodes::INCORRECT_DATA, "ORC file footer is corrupt: type nesting depth exceeds the limit of {}", MAX_TYPE_NESTING_DEPTH);

    /// The types are listed in pre-order and the column id of each type is its index in the list.
    if (index != next_id || index >= types.size())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ORC file footer is corrupt: the types are not a tree listed in pre-order (type {} is referenced at position {})",
            index, next_id);
    ++next_id;

    const auto & proto = types[index];
    if (proto.kind > GEOGRAPHY)
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown ORC type kind {}", proto.kind);

    auto kind = static_cast<TypeKind>(proto.kind);
    auto result = std::make_unique<Type>(kind);

    switch (kind)
    {
        case LIST:
            if (proto.subtypes.size() != 1)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Illegal ORC LIST type that doesn't contain one subtype");
            break;
        case MAP:
            if (proto.subtypes.size() != 2)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Illegal ORC MAP type that doesn't contain two subtypes");
            break;
        case UNION:
            if (proto.subtypes.empty())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Illegal ORC UNION type that doesn't contain any subtypes");
            /// The tags of a union are bytes.
            if (proto.subtypes.size() > 256)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Illegal ORC UNION type with {} subtypes, more than 256", proto.subtypes.size());
            break;
        case STRUCT:
            if (proto.subtypes.size() > proto.field_names.size())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Illegal ORC STRUCT type that contains less field names than subtypes");
            break;
        default:
            if (!proto.subtypes.empty())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Illegal ORC type {} with subtypes", proto.kind);
            break;
    }

    if (kind == CHAR || kind == VARCHAR)
        result->setMaximumLength(proto.maximum_length.value_or(0));
    if (kind == DECIMAL)
        result->setPrecisionAndScale(proto.precision.value_or(0), proto.scale.value_or(0));
    if (kind == GEOMETRY || kind == GEOGRAPHY)
        result->setGeospatial(proto.crs.value_or("OGC:CRS84"), proto.algorithm.value_or(0));

    for (size_t i = 0; i < proto.subtypes.size(); ++i)
    {
        auto child = convertType(types, proto.subtypes[i], next_id, depth + 1);
        if (kind == STRUCT)
            result->addStructField(proto.field_names[i], std::move(child));
        else
            result->addChild(std::move(child));
    }

    for (const auto & attribute : proto.attributes)
        result->setAttribute(attribute.key, attribute.value);

    return result;
}

void selectChildren(std::vector<bool> & selected, const Type & type)
{
    for (UInt64 c = type.getColumnId(); c <= type.getMaximumColumnId(); ++c)
        selected[c] = true;
}

bool selectParents(std::vector<bool> & selected, const Type & type)
{
    size_t id = type.getColumnId();
    bool result = selected[id];
    size_t num_selected_subtypes = 0;
    for (UInt64 c = 0; c < type.getSubtypeCount(); ++c)
    {
        if (selectParents(selected, *type.getSubtype(c)))
        {
            result = true;
            ++num_selected_subtypes;
        }
    }
    selected[id] = result;

    /// The branches of a union are either all selected or all not selected.
    if (type.getKind() == UNION && result && num_selected_subtypes > 0 && num_selected_subtypes < type.getSubtypeCount())
        for (UInt64 c = 0; c < type.getSubtypeCount(); ++c)
            selectChildren(selected, *type.getSubtype(c));

    return result;
}

}


std::string Type::toString() const
{
    switch (kind)
    {
        case BOOLEAN: return "boolean";
        case BYTE: return "tinyint";
        case SHORT: return "smallint";
        case INT: return "int";
        case LONG: return "bigint";
        case FLOAT: return "float";
        case DOUBLE: return "double";
        case STRING: return "string";
        case BINARY: return "binary";
        case TIMESTAMP: return "timestamp";
        case TIMESTAMP_INSTANT: return "timestamp with local time zone";
        case LIST: return "array<" + (subtypes.empty() ? std::string("void") : subtypes[0]->toString()) + ">";
        case MAP:
            return "map<" + (subtypes.empty() ? std::string("void") : subtypes[0]->toString()) + ","
                + (subtypes.size() < 2 ? std::string("void") : subtypes[1]->toString()) + ">";
        case STRUCT:
        {
            std::string result = "struct<";
            for (size_t i = 0; i < subtypes.size(); ++i)
            {
                if (i != 0)
                    result += ",";
                if (isUnquotedFieldName(field_names[i]))
                    result += field_names[i];
                else
                {
                    result += "`";
                    for (char c : field_names[i])
                    {
                        if (c == '`')
                            result += "``";
                        else
                            result += c;
                    }
                    result += "`";
                }
                result += ":";
                result += subtypes[i]->toString();
            }
            result += ">";
            return result;
        }
        case UNION:
        {
            std::string result = "uniontype<";
            for (size_t i = 0; i < subtypes.size(); ++i)
            {
                if (i != 0)
                    result += ",";
                result += subtypes[i]->toString();
            }
            result += ">";
            return result;
        }
        case DECIMAL: return "decimal(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
        case DATE: return "date";
        case VARCHAR: return "varchar(" + std::to_string(maximum_length) + ")";
        case CHAR: return "char(" + std::to_string(maximum_length) + ")";
        case GEOMETRY: return "geometry(" + crs + ")";
        case GEOGRAPHY: return "geography(" + crs + "," + algorithmToString(algorithm) + ")";
    }
    return "unknown";
}

Type * Type::addStructField(const std::string & name, std::unique_ptr<Type> field)
{
    field_names.push_back(name);
    return addChild(std::move(field));
}

Type * Type::addChild(std::unique_ptr<Type> child)
{
    subtypes.push_back(std::move(child));
    return subtypes.back().get();
}

UInt64 Type::assignIds(UInt64 start)
{
    column_id = start;
    UInt64 next = start + 1;
    for (auto & subtype : subtypes)
        next = subtype->assignIds(next);
    maximum_column_id = next - 1;
    return next;
}

void Type::toProto(std::vector<Proto::Type> & out) const
{
    size_t index = out.size();
    if (index != column_id)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC type ids are not assigned in pre-order");
    out.emplace_back();
    {
        auto & proto = out.back();
        proto.kind = kind;
        for (const auto & subtype : subtypes)
            proto.subtypes.push_back(subtype->getColumnId());
        proto.field_names = field_names;
        if (kind == CHAR || kind == VARCHAR)
            proto.maximum_length = static_cast<UInt32>(maximum_length);
        if (kind == DECIMAL)
        {
            proto.precision = static_cast<UInt32>(precision);
            proto.scale = static_cast<UInt32>(scale);
        }
        for (const auto & [key, value] : attributes)
            proto.attributes.push_back({key, value});
    }
    /// `out` may be reallocated by the recursive calls, so do not keep the reference.
    for (const auto & subtype : subtypes)
        subtype->toProto(out);
}

std::unique_ptr<Type> Type::createPrimitiveType(TypeKind kind)
{
    return std::make_unique<Type>(kind);
}

std::unique_ptr<Type> Type::createCharType(TypeKind kind, UInt64 maximum_length)
{
    auto result = std::make_unique<Type>(kind);
    result->maximum_length = maximum_length;
    return result;
}

std::unique_ptr<Type> Type::createDecimalType(UInt64 precision, UInt64 scale)
{
    auto result = std::make_unique<Type>(DECIMAL);
    result->precision = precision;
    result->scale = scale;
    return result;
}

std::unique_ptr<Type> Type::createStructType()
{
    return std::make_unique<Type>(STRUCT);
}

std::unique_ptr<Type> Type::createListType(std::unique_ptr<Type> element)
{
    auto result = std::make_unique<Type>(LIST);
    result->addChild(std::move(element));
    return result;
}

std::unique_ptr<Type> Type::createMapType(std::unique_ptr<Type> key, std::unique_ptr<Type> value)
{
    auto result = std::make_unique<Type>(MAP);
    result->addChild(std::move(key));
    result->addChild(std::move(value));
    return result;
}

std::unique_ptr<Type> Type::createUnionType()
{
    return std::make_unique<Type>(UNION);
}

std::unique_ptr<Type> Type::fromProto(const std::vector<Proto::Type> & types)
{
    if (types.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "ORC file footer is corrupt: it has no types");
    UInt64 next_id = 0;
    auto root = convertType(types, 0, next_id, 0);
    if (next_id != types.size())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ORC file footer is corrupt: {} of {} types are not reachable from the root type", types.size() - next_id, types.size());
    root->assignIds(0);
    return root;
}

std::unique_ptr<Type> Type::buildSelectedType(const Type & type, const std::vector<bool> & selected)
{
    if (!selected.at(type.column_id))
        return nullptr;

    auto result = std::make_unique<Type>(type.kind);
    result->maximum_length = type.maximum_length;
    result->precision = type.precision;
    result->scale = type.scale;
    result->crs = type.crs;
    result->algorithm = type.algorithm;
    result->attributes = type.attributes;
    result->column_id = type.column_id;
    result->maximum_column_id = type.maximum_column_id;

    for (size_t i = 0; i < type.subtypes.size(); ++i)
    {
        auto child = buildSelectedType(*type.subtypes[i], selected);
        if (!child)
            continue;
        if (type.kind == STRUCT)
            result->addStructField(type.field_names[i], std::move(child));
        else
            result->addChild(std::move(child));
    }
    return result;
}

std::vector<bool> selectColumnsByTypeIds(const Type & root, const std::vector<UInt64> & type_ids)
{
    std::vector<bool> selected(root.getMaximumColumnId() + 1, false);

    /// Find the types by ids with a walk over the tree.
    std::vector<const Type *> by_id(selected.size());
    std::vector<const Type *> stack{&root};
    while (!stack.empty())
    {
        const Type * type = stack.back();
        stack.pop_back();
        by_id[type->getColumnId()] = type;
        for (UInt64 i = 0; i < type->getSubtypeCount(); ++i)
            stack.push_back(type->getSubtype(i));
    }

    for (UInt64 id : type_ids)
    {
        if (id >= selected.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid ORC type id {} selected out of {}", id, selected.size());
        selectChildren(selected, *by_id[id]);
    }

    selectParents(selected, root);
    selected[0] = true;
    return selected;
}

}
