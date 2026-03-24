#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTWithAlias.h>
#include <Core/Field.h>
#include <Common/Exception.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>

namespace DB
{

/// Helper for reading AST nodes from JSON objects.
/// Provides convenient methods symmetric to JSONObjectWriter.
class JSONObjectReader
{
public:
    explicit JSONObjectReader(const Poco::JSON::Object & obj_) : obj(obj_) {}

    String getString(const char * key, const String & default_value = {}) const
    {
        if (obj.has(key))
            return obj.getValue<String>(key);
        return default_value;
    }

    bool getBool(const char * key, bool default_value = false) const
    {
        if (obj.has(key))
            return obj.getValue<bool>(key);
        return default_value;
    }

    Int64 getInt(const char * key, Int64 default_value = 0) const
    {
        if (obj.has(key))
            return obj.getValue<Int64>(key);
        return default_value;
    }

    UInt64 getUInt(const char * key, UInt64 default_value = 0) const
    {
        if (obj.has(key))
            return obj.getValue<Poco::UInt64>(key);
        return default_value;
    }

    double getDouble(const char * key, double default_value = 0) const
    {
        if (obj.has(key))
            return obj.getValue<double>(key);
        return default_value;
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
        const auto & child_obj = child_var.extract<Poco::JSON::Object::Ptr>();
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
            return result;
        result.reserve(arr->size());
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            const auto & child_obj = arr->getObject(i);
            result.push_back(IAST::createFromJSON(*child_obj));
        }
        return result;
    }

    /// Read a Field value from a nested JSON object.
    Field readField(const char * key) const
    {
        if (!obj.has(key))
            return Field();
        auto field_obj = obj.getObject(key);
        if (!field_obj)
            return Field();
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
    }

    /// Read a JSON array of strings.
    std::vector<String> readStringArray(const char * key) const
    {
        std::vector<String> result;
        if (!obj.has(key))
            return result;
        auto arr = obj.getArray(key);
        if (!arr)
            return result;
        for (unsigned int i = 0; i < arr->size(); ++i)
            result.push_back(arr->getElement<String>(i));
        return result;
    }

    /// Get the underlying Poco JSON object.
    const Poco::JSON::Object & getObject() const { return obj; }

    /// Get a nested JSON array.
    Poco::JSON::Array::Ptr getArray(const char * key) const
    {
        if (!obj.has(key))
            return nullptr;
        return obj.getArray(key);
    }

    /// Get a nested JSON object.
    Poco::JSON::Object::Ptr getNestedObject(const char * key) const
    {
        if (!obj.has(key))
            return nullptr;
        return obj.getObject(key);
    }

    static Field readFieldFromObject(const Poco::JSON::Object & field_obj);

private:
    const Poco::JSON::Object & obj;
};

}
