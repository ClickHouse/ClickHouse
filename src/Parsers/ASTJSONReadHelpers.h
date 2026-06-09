#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTQueryParameter.h>
#include <Core/Field.h>
#include <Common/Exception.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Helper for reading AST nodes from JSON objects.
/// Provides convenient methods symmetric to JSONObjectWriter.
class JSONObjectReader
{
public:
    explicit JSONObjectReader(const Poco::JSON::Object & obj_) : obj(obj_) {}

    /// The scalar getters below validate the JSON scalar type strictly before extracting.
    /// `Poco::Dynamic::Var` otherwise coerces freely (e.g. the string `"yes"` becomes
    /// `true`, `"123"` becomes a number), which would let malformed `clickhouse_json`
    /// deserialize into a different valid AST instead of being rejected at the boundary.
    /// An absent key still returns the default (presence is enforced separately by callers
    /// that require a field). Note: Poco reports a JSON boolean as `isInteger()`/`isNumeric()`
    /// too (`bool` is an integral type), so the numeric getters exclude `isBoolean()`.
    String getString(const char * key, const String & default_value = {}) const
    {
        if (!obj.has(key))
            return default_value;
        if (!obj.get(key).isString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a string for key '{}' during AST JSON deserialization", key);
        return obj.getValue<String>(key);
    }

    bool getBool(const char * key, bool default_value = false) const
    {
        if (!obj.has(key))
            return default_value;
        if (!obj.get(key).isBoolean())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a boolean for key '{}' during AST JSON deserialization", key);
        return obj.getValue<bool>(key);
    }

    Int64 getInt(const char * key, Int64 default_value = 0) const
    {
        if (!obj.has(key))
            return default_value;
        const auto var = obj.get(key);
        if (!var.isInteger() || var.isBoolean())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected an integer for key '{}' during AST JSON deserialization", key);
        return obj.getValue<Int64>(key);
    }

    UInt64 getUInt(const char * key, UInt64 default_value = 0) const
    {
        if (!obj.has(key))
            return default_value;
        const auto var = obj.get(key);
        if (!var.isInteger() || var.isBoolean())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected an unsigned integer for key '{}' during AST JSON deserialization", key);
        return obj.getValue<Poco::UInt64>(key);
    }

    double getDouble(const char * key, double default_value = 0) const
    {
        if (!obj.has(key))
            return default_value;
        const auto var = obj.get(key);
        if (!var.isNumeric() || var.isBoolean())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a number for key '{}' during AST JSON deserialization", key);
        return obj.getValue<double>(key);
    }

    bool has(const char * key) const { return obj.has(key); }

    /// Read a child AST node. Returns nullptr if key doesn't exist.
    ASTPtr readChild(const char * key) const
    {
        if (!obj.has(key))
            return nullptr;
        auto child_var = obj.get(key);
        if (child_var.isEmpty())
            return nullptr;
        Poco::JSON::Object::Ptr child_obj;
        try
        {
            child_obj = child_var.extract<Poco::JSON::Object::Ptr>();
        }
        catch (const Poco::Exception &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected JSON object for key '{}' during AST JSON deserialization", key);
        }
        if (!child_obj)
            return nullptr;
        return IAST::createFromJSON(*child_obj);
    }

    /// Read the "children" array.
    ASTs readChildren() const
    {
        ASTs result;
        if (!obj.has("children"))
            return result;
        auto arr = obj.getArray("children");
        if (!arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'children' is not a JSON array during AST JSON deserialization");
        result.reserve(arr->size());
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto child_obj = arr->getObject(i);
            if (!child_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'children' array during AST JSON deserialization", i);
            result.push_back(IAST::createFromJSON(*child_obj));
        }
        return result;
    }

    /// Read the "children" array and require every element to be of the concrete AST type `T`.
    /// Typed-container nodes (e.g. `TablesInSelectQuery`, `TableOverrideList`) downcast their
    /// children unconditionally during semantic processing, so an unexpected child type from
    /// malformed `clickhouse_json` must be rejected here instead of reaching a
    /// `typeid_cast`/internal-exception path (or being silently dropped) later.
    template <typename T>
    ASTs readChildrenOfType(std::string_view node_name) const
    {
        ASTs result = readChildren();
        for (const auto & child : result)
            if (!child || !child->as<T>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Unexpected child node type in {} during AST JSON deserialization", node_name);
        return result;
    }

    /// Read a Field value from a nested JSON object.
    /// Returns a default `Field` when the key is absent.
    /// Throws `BAD_ARGUMENTS` when the key exists but its value is not a JSON object,
    /// to avoid silently turning malformed input (e.g. a string) into `Null`.
    Field readField(const char * key) const
    {
        if (!obj.has(key))
            return Field();
        auto field_obj = obj.getObject(key);
        if (!field_obj)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected JSON object for key '{}' (Field value) during AST JSON deserialization", key);
        return readFieldFromObject(*field_obj);
    }

    /// Read an alias and prefer_alias_to_column_name (for ASTWithAlias nodes).
    void readAlias(ASTWithAlias & node) const
    {
        String alias = getString("alias");
        if (!alias.empty())
            node.setAlias(alias);
        if (getBool("prefer_alias_to_column_name"))
            node.setPreferAliasToColumnName(true);
        if (has("parametrised_alias"))
        {
            auto param_ast = readChild("parametrised_alias");
            if (param_ast)
            {
                node.parametrised_alias = boost::dynamic_pointer_cast<ASTQueryParameter>(param_ast);
                if (!node.parametrised_alias)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected ASTQueryParameter for 'parametrised_alias' during AST JSON deserialization");
            }
        }
    }

    /// Read a JSON array of strings.
    /// When the key exists but its value is not a JSON array, throws `BAD_ARGUMENTS`
    /// instead of silently returning an empty vector — silently dropping fields like
    /// `name_parts` or `src_replicas` would convert malformed input into a different
    /// valid AST. Each element must be a JSON string; otherwise a `BAD_ARGUMENTS`
    /// exception is thrown (Poco's `getElement<String>` would otherwise silently
    /// stringify numbers, bools, etc.).
    std::vector<String> readStringArray(const char * key) const
    {
        std::vector<String> result;
        if (!obj.has(key))
            return result;
        auto arr = obj.getArray(key);
        if (!arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected JSON array for key '{}' during AST JSON deserialization", key);
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto var = arr->get(i);
            if (!var.isString())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Element at index {} of JSON array '{}' is not a string during AST JSON deserialization", i, key);
            result.push_back(var.extract<String>());
        }
        return result;
    }

    /// Get the underlying Poco JSON object.
    const Poco::JSON::Object & getObject() const { return obj; }

    /// Get a nested JSON array.
    /// Returns nullptr when the key is absent.
    /// Throws `BAD_ARGUMENTS` when the key exists but its value is not a JSON array,
    /// so callers using the `if (arr)` / `if (!arr) return;` shapes do not silently
    /// drop malformed input by treating wrong-type values as if the key were missing.
    Poco::JSON::Array::Ptr getArray(const char * key) const
    {
        if (!obj.has(key))
            return nullptr;
        auto arr = obj.getArray(key);
        if (!arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected JSON array for key '{}' during AST JSON deserialization", key);
        return arr;
    }

    /// Get a nested JSON object.
    /// Returns nullptr when the key is absent.
    /// Throws `BAD_ARGUMENTS` when the key exists but its value is not a JSON object,
    /// so callers do not silently accept malformed input by treating wrong-type values
    /// as if the key were missing.
    Poco::JSON::Object::Ptr getNestedObject(const char * key) const
    {
        if (!obj.has(key))
            return nullptr;
        auto nested = obj.getObject(key);
        if (!nested)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected JSON object for key '{}' during AST JSON deserialization", key);
        return nested;
    }

    static Field readFieldFromObject(const Poco::JSON::Object & field_obj);

private:
    /// Recursive worker for `readFieldFromObject`. `depth` tracks the nesting level of
    /// structured `Field` values (Array/Tuple/Map). A hostile `Literal` node can embed
    /// deeply nested `{"field_type":"Array","value":[...]}` levels that add no AST nodes,
    /// so the AST depth/element limits and `checkDepth` do not bound this recursion.
    static Field readFieldFromObjectImpl(const Poco::JSON::Object & field_obj, size_t depth);

    const Poco::JSON::Object & obj;
};

}
