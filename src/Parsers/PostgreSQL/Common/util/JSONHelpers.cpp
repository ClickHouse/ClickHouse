#include "JSONHelpers.h"

namespace DB::PostgreSQL
{
    Value::Value() {}
    Value::Value(const Field& field) : primitive(field), type(NodeType::Primitive) {}
    Value::Value(const NodeArray& arr, NodeType _type) : type(_type) 
    {
        assert(type == NodeType::Array || type == NodeType::Object);
        array_or_object = arr;
    }
    
    NodeType Value::GetType() const 
    {
        return type;
    }
    std::optional<Field> Value::GetPrimitive() const 
    { 
        if (type == NodeType::Primitive)
        {
            return primitive;
        } 
        else 
        {
            return std::nullopt; 
        }
    }
    std::optional<NodeArray> Value::GetArrayOrObject() const 
    { 
        if (type == NodeType::Array || type == NodeType::Object) 
        {
            return array_or_object;
        }
        else
        {
            return std::nullopt;
        }
    }

    Node::Node() {}
    Node::Node(const std::string& key_, const Value& value_) : key(key_), value(value_) {}
    Node::Node(const Value& value_) : value(value_) {}

    bool Node::HasChildWithKey(const std::string& key_) const 
    {
        std::optional<NodeArray> arrayOrObject = value->GetArrayOrObject();
        if (arrayOrObject == std::nullopt) 
        {
            return false;
        }
        if (arrayOrObject.has_value()) 
        {
            for (const auto& childNode : arrayOrObject.value()) 
            {
                if (childNode->GetKey().has_value() && childNode->GetKey().value() == key_) 
                {
                    return true;
                }
            }
        }

        return false;
    }

    std::shared_ptr<Node> Node::GetChildWithKey(const std::string& key_) const 
    {
        std::optional<NodeArray> arrayOrObject = value->GetArrayOrObject();
        if (arrayOrObject == std::nullopt) 
        {
            return nullptr;
        }
        if (arrayOrObject.has_value()) 
        {
            for (const auto& childNode : arrayOrObject.value()) 
            {
                if (childNode->GetKey().has_value() && childNode->GetKey().value() == key_) 
                {
                    return childNode;
                }
            }
        }

        return nullptr;
    }


    std::shared_ptr<Node> Node::operator[](const std::string& key_) const 
    {
        return GetChildWithKey(key_);
    }

    std::vector<std::string> Node::ListChildKeys() const 
    {
        std::vector<std::string> res;
        std::optional<NodeArray> arrayOrObject = value->GetArrayOrObject();
        if (arrayOrObject == std::nullopt) 
        {
            return res;
        }
        if (arrayOrObject.has_value()) {
            for (const auto& childNode : arrayOrObject.value()) {
                if (childNode->GetKey().has_value()) 
                {
                    res.push_back(childNode->GetKey().value());
                }
            }
        }
        return res;
    }

    void Node::SetKey(const std::string& key_) 
    {
        key = key_; 
    }
    void Node::SetValue(const Value& value_)
    {
        value = value_; 
    }

    std::optional<std::string> Node::GetKey() const 
    {
        return key;
    }

    std::string Node::GetKeyString() const 
    {
        const auto& key_ = GetKey();
        if (key_ == std::nullopt)
        {
            return "";
        }
        return key_.value();
    }

    std::optional<Value> Node::GetValue() const 
    {
        return value; 
    }

    NodeArray Node::GetNodeArray() const {
        assert(GetType() == NodeType::Array || GetType() == NodeType::Object);
        return value.value().GetArrayOrObject().value();
    }

    NodeType Node::GetType() const {
        if (!value.has_value()) {
            return NodeType::Undefined;
        }
        return value.value().GetType();
    }

    namespace {
        std::shared_ptr<Node> GetPrimitiveValueNode(const JSON::Element& elem) {
            Value value;
            if (elem.isInt64()) {
                Field field = Field(elem.getInt64());
                value = Value(field);
            } else if (elem.isUInt64()) {
                Field field = Field(elem.getUInt64());
                value = Value(field);
            } else if (elem.isDouble()) {
                Field field = Field(elem.getDouble());
                value = Value(field);
            } else if (elem.isString()) {
                Field field = Field(elem.getString());
                value = Value(field);
            } else if (elem.isBool()) {
                Field field = Field(elem.getBool());
                value = Value(field);
            } else if (elem.isNull()) {
                value = Value(Field());
            }
            std::shared_ptr<Node> node = std::make_shared<Node>(value);
            return node;
        }
    }

    std::shared_ptr<Node> buildJSONTree(const JSON::Element& elem)
    {
        switch (elem.type()) 
        {
            case ElementType::OBJECT:
                {
                    std::cerr << "Object node\n";
                    NodeArray children;
                    const auto& obj = elem.getObject();
                    for (const auto [key, value] : obj) {
                        children.push_back(buildJSONTree(value));
                        children.back()->SetKey(std::string(key));
                    }
                    Value value(children, NodeType::Object);
                    return std::make_shared<Node>(value);
                }
            case ElementType::ARRAY:
                {
                    std::cerr << "Array node\n";
                    NodeArray children;
                    const auto arr = elem.getArray();
                    for (const auto a : arr)
                    {
                        children.push_back(buildJSONTree(a));
                    }
                    Value value(children, NodeType::Array);
                    return std::make_shared<Node>(value);
                }
            default:
                std::cerr << "Primitive node\n";
                return GetPrimitiveValueNode(elem);
        }
    }

    void PrintDebugInfoRecursive(std::shared_ptr<Node> node) {
        PrintDebugInfo(node);
        if (node->GetType() == NodeType::Array || node->GetType() == NodeType::Object) {
            NodeArray arrayOrObject = node->GetNodeArray();
            for (const auto& child : arrayOrObject) {
                PrintDebugInfoRecursive(child);
            }
        }
    }

    void PrintDebugInfo(std::shared_ptr<Node> node) {
        std::cerr << "___________________________";
        std::cerr << "Node info\n";
        if (!node) {
            std::cerr << "nullptr\n";
            std::cerr << "___________________________";
            return;
        }
        std::cerr << "Key: " << node->GetKeyString() << std::endl;
        const auto& valueOpt = node->GetValue();
        if (valueOpt == std::nullopt) {
            std::cerr << "Value: nullopt\n";
            std::cerr << "___________________________";
            return;
        }
        const auto& value = valueOpt.value();
        if (value.GetType() == NodeType::Primitive) {
            const auto& primitiveOpt = value.GetPrimitive();
            if (primitiveOpt == std::nullopt) {
                std::cerr << "Value: primitive nullopt\n";
            } else {
                std::cerr << "Value: " << toString(primitiveOpt.value()) << std::endl;
            }
            std::cerr << "___________________________";
            return;
        }
        std::cerr << "Value: array_or_object\n";
        const auto keys = node->ListChildKeys();
        std::cerr << "Child Keys: \n";
        for (auto key : keys) {
            std::cerr << key << std::endl;
        }
        std::cerr << "___________________________";
    }
}
