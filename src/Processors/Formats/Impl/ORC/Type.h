#pragma once

#include <Processors/Formats/Impl/ORC/Proto.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace DB::ORC
{

enum TypeKind : UInt32
{
    BOOLEAN = 0,
    BYTE = 1,
    SHORT = 2,
    INT = 3,
    LONG = 4,
    FLOAT = 5,
    DOUBLE = 6,
    STRING = 7,
    BINARY = 8,
    TIMESTAMP = 9,
    LIST = 10,
    MAP = 11,
    STRUCT = 12,
    UNION = 13,
    DECIMAL = 14,
    DATE = 15,
    VARCHAR = 16,
    CHAR = 17,
    TIMESTAMP_INSTANT = 18,
    GEOMETRY = 19,
    GEOGRAPHY = 20,
};

/// A node of the type tree of an ORC file. Every node is a column of the file; the column ids are
/// assigned in pre-order, so the subtree of a column `c` occupies the ids `[c, c.getMaximumColumnId()]`.
class Type
{
public:
    explicit Type(TypeKind kind_) : kind(kind_) {}

    TypeKind getKind() const { return kind; }
    UInt64 getColumnId() const { return column_id; }
    UInt64 getMaximumColumnId() const { return maximum_column_id; }
    UInt64 getSubtypeCount() const { return subtypes.size(); }
    const Type * getSubtype(UInt64 i) const { return subtypes.at(i).get(); }
    /// Only for STRUCT.
    const std::string & getFieldName(UInt64 i) const { return field_names.at(i); }
    /// Only for CHAR and VARCHAR.
    UInt64 getMaximumLength() const { return maximum_length; }
    /// Only for DECIMAL.
    UInt64 getPrecision() const { return precision; }
    UInt64 getScale() const { return scale; }

    bool hasAttributeKey(const std::string & key) const { return attributes.contains(key); }
    const std::string & getAttributeValue(const std::string & key) const { return attributes.at(key); }
    void setAttribute(const std::string & key, const std::string & value) { attributes[key] = value; }
    const std::map<std::string, std::string> & getAttributes() const { return attributes; }

    /// The type name in the same format as in the reference implementation (Hive type names).
    std::string toString() const;

    void setMaximumLength(UInt64 value) { maximum_length = value; }
    void setPrecisionAndScale(UInt64 precision_, UInt64 scale_) { precision = precision_; scale = scale_; }
    void setGeospatial(std::string crs_, UInt32 algorithm_) { crs = std::move(crs_); algorithm = algorithm_; }

    Type * addStructField(const std::string & name, std::unique_ptr<Type> field);
    Type * addChild(std::unique_ptr<Type> child);

    /// Assigns the column ids in pre-order, starting from `start`. Returns the next free id.
    UInt64 assignIds(UInt64 start);

    /// Appends the types of this subtree to `out` in pre-order. The ids must be assigned.
    void toProto(std::vector<Proto::Type> & out) const;

    static std::unique_ptr<Type> createPrimitiveType(TypeKind kind);
    static std::unique_ptr<Type> createCharType(TypeKind kind, UInt64 maximum_length);
    static std::unique_ptr<Type> createDecimalType(UInt64 precision, UInt64 scale);
    static std::unique_ptr<Type> createStructType();
    static std::unique_ptr<Type> createListType(std::unique_ptr<Type> element);
    static std::unique_ptr<Type> createMapType(std::unique_ptr<Type> key, std::unique_ptr<Type> value);
    static std::unique_ptr<Type> createUnionType();

    /// Builds the type tree from the types in the file footer. Throws `INCORRECT_DATA` if they do
    /// not form a tree with the ids in pre-order.
    static std::unique_ptr<Type> fromProto(const std::vector<Proto::Type> & types);

    /// Builds a copy of the tree with only the selected columns (by id). The parent of every
    /// selected column must also be selected. Returns nullptr if the root is not selected.
    static std::unique_ptr<Type> buildSelectedType(const Type & type, const std::vector<bool> & selected);

private:
    TypeKind kind;
    UInt64 column_id = 0;
    UInt64 maximum_column_id = 0;
    std::vector<std::unique_ptr<Type>> subtypes;
    std::vector<std::string> field_names;
    UInt64 maximum_length = 0;
    UInt64 precision = 0;
    UInt64 scale = 0;
    std::string crs;
    UInt32 algorithm = 0;
    std::map<std::string, std::string> attributes;
};

/// The column selection with the same semantics as in the reference implementation:
/// selecting a column selects its whole subtree, and then all parents of the selected columns
/// are selected. If only some branches of a UNION are selected, all its branches are selected.
std::vector<bool> selectColumnsByTypeIds(const Type & root, const std::vector<UInt64> & type_ids);

}
