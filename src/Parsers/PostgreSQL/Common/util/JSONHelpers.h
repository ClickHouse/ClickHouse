#pragma once

#include <Parsers/PostgreSQL/Common/Types.h>

#include <optional>
#include <iostream>

#include <Common/Exception.h>
#include <Core/Field.h>

namespace DB::PostgreSQL
{
    enum class NodeType { 
        Primitive = 0, 
        Array = 1, 
        Object = 2 
    };

    class Node;

    using NodeArray = std::vector<std::shared_ptr<Node>>;

    class Value {
    public:
        Value();
        explicit Value(const Field& field);
        explicit Value(const NodeArray& arr, NodeType type_);
        
        NodeType GetType() const;

        std::optional<Field> GetPrimitive() const;
        std::optional<NodeArray> GetArrayOrObject() const;

    private:
        std::optional<Field> primitive;
        std::optional<NodeArray> array_or_object;
        NodeType type;
    };

    class Node {
    public:
        Node();
        explicit Node(const std::string& key_, const Value& value_);
        explicit Node(const Value& value_);

        bool HasChildWithKey(const std::string& key_) const;
        std::shared_ptr<Node> GetChildWithKey(const std::string& key_) const;

        std::vector<std::string> ListChildKeys() const;

        std::optional<std::string> GetKey() const;
        std::optional<Value> GetValue() const;
        void SetKey(const std::string& key_);
        void SetValue(const Value& value_);

    private:
        std::optional<std::string> key;
        std::optional<Value> value;
    };

    std::shared_ptr<Node> buildJSONTree(const JSON::Element& elem);
}
