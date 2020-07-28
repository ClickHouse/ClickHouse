#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_RAPIDJSON
#    include <Core/Types.h>
#    include <Common/Exception.h>
#    include <common/StringRef.h>

#    include <rapidjson/document.h>


namespace DB
{

/// This class can be used as an argument for the template class FunctionJSON.
/// It provides ability to parse JSONs using rapidjson library.
struct RapidJSONParser
{
    static constexpr bool need_preallocate = false;
    void preallocate(size_t) {}

    bool parse(const StringRef & json)
    {
        rapidjson::MemoryStream ms(json.data, json.size);
        rapidjson::EncodedInputStream<rapidjson::UTF8<>, rapidjson::MemoryStream> is(ms);
        document.ParseStream(is);
        return !document.HasParseError() && (ms.Tell() == json.size);
    }

    struct Iterator
    {
    public:
        Iterator() {}
        Iterator(const rapidjson::Document & document_) : value(&document_) {}
        Iterator(const Iterator & src)
            : value(src.value)
            , is_object_member(src.is_object_member)
            , current_in_array(src.current_in_array)
            , end_of_array(src.end_of_array) {}

        Iterator & operator =(const Iterator & src)
        {
            value = src.value;
            is_object_member = src.is_object_member;
            current_in_array = src.current_in_array;
            end_of_array = src.end_of_array;
            return *this;
        }

        bool isInt64() const { return value->IsInt64(); }
        bool isUInt64() const { return value->IsUint64(); }
        bool isDouble() const { return value->IsDouble(); }
        bool isBool() const { return value->IsBool(); }
        bool isString() const { return value->IsString(); }
        bool isArray() const { return value->IsArray(); }
        bool isObject() const { return value->IsObject(); }
        bool isNull() const { return value->IsNull(); }

        Int64 getInt64() const { return value->GetInt64(); }
        UInt64 getUInt64() const { return value->GetUint64(); }
        double getDouble() const { return value->GetDouble(); }
        bool getBool() const { return value->GetBool(); }
        StringRef getString() const { return {value->GetString(), value->GetStringLength()}; }

        size_t sizeOfArray() const { return value->Size(); }

        bool arrayElementByIndex(size_t index)
        {
            if (index >= value->Size())
                return false;
            setRange(value->Begin() + index, value->End());
            value = current_in_array++;
            return true;
        }

        bool nextArrayElement()
        {
            if (current_in_array == end_of_array)
                return false;
            value = current_in_array++;
            return true;
        }

        size_t sizeOfObject() const { return value->MemberCount(); }

        bool objectMemberByIndex(size_t index)
        {
            if (index >= value->MemberCount())
                return false;
            setRange(value->MemberBegin() + index, value->MemberEnd());
            value = &(current_in_object++)->value;
            return true;
        }

        bool objectMemberByIndex(size_t index, StringRef & key)
        {
            if (index >= value->MemberCount())
                return false;
            setRange(value->MemberBegin() + index, value->MemberEnd());
            key = getKeyImpl(current_in_object);
            value = &(current_in_object++)->value;
            return true;
        }

        bool objectMemberByName(const StringRef & name)
        {
            auto it = value->FindMember(name.data);
            if (it == value->MemberEnd())
                return false;
            setRange(it, value->MemberEnd());
            value = &(current_in_object++)->value;
            return true;
        }

        bool nextObjectMember()
        {
            if (current_in_object == end_of_object)
                return false;
            value = &(current_in_object++)->value;
            return true;
        }

        bool nextObjectMember(StringRef & key)
        {
            if (current_in_object == end_of_object)
                return false;
            key = getKeyImpl(current_in_object);
            value = &(current_in_object++)->value;
            return true;
        }

        bool isObjectMember() const { return is_object_member; }

        StringRef getKey() const
        {
            return getKeyImpl(current_in_object - 1);
        }

    private:
        void setRange(rapidjson::Value::ConstValueIterator current, rapidjson::Value::ConstValueIterator end)
        {
            current_in_array = &*current;
            end_of_array = &*end;
            is_object_member = false;
        }

        void setRange(rapidjson::Value::ConstMemberIterator current, rapidjson::Value::ConstMemberIterator end)
        {
            current_in_object = &*current;
            end_of_object = &*end;
            is_object_member = true;
        }

        static StringRef getKeyImpl(const rapidjson::GenericMember<rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<>> * member)
        {
            const auto & name = member->name;
            return {name.GetString(), name.GetStringLength()};
        }

        const rapidjson::Value * value = nullptr;
        bool is_object_member = false;

        union
        {
            const rapidjson::GenericMember<rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<>> * current_in_object;
            const rapidjson::Value * current_in_array;
        };
        union
        {
            const rapidjson::GenericMember<rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<>> * end_of_object;
            const rapidjson::Value * end_of_array;
        };
    };

    Iterator getRoot() { return Iterator{document}; }

    static bool isInt64(const Iterator & it) { return it.isInt64(); }
    static bool isUInt64(const Iterator & it) { return it.isUInt64(); }
    static bool isDouble(const Iterator & it) { return it.isDouble(); }
    static bool isBool(const Iterator & it) { return it.isBool(); }
    static bool isString(const Iterator & it) { return it.isString(); }
    static bool isArray(const Iterator & it) { return it.isArray(); }
    static bool isObject(const Iterator & it) { return it.isObject(); }
    static bool isNull(const Iterator & it) { return it.isNull(); }

    static Int64 getInt64(const Iterator & it) { return it.getInt64(); }
    static UInt64 getUInt64(const Iterator & it) { return it.getUInt64(); }
    static double getDouble(const Iterator & it) { return it.getDouble(); }
    static bool getBool(const Iterator & it) { return it.getBool(); }
    static StringRef getString(const Iterator & it) { return it.getString(); }

    static size_t sizeOfArray(const Iterator & it) { return it.sizeOfArray(); }
    static bool firstArrayElement(Iterator & it) { return it.arrayElementByIndex(0); }
    static bool arrayElementByIndex(Iterator & it, size_t index) { return it.arrayElementByIndex(index); }
    static bool nextArrayElement(Iterator & it) { return it.nextArrayElement(); }

    static size_t sizeOfObject(const Iterator & it) { return it.sizeOfObject(); }
    static bool firstObjectMember(Iterator & it) { return it.objectMemberByIndex(0); }
    static bool firstObjectMember(Iterator & it, StringRef & first_key) { return it.objectMemberByIndex(0, first_key); }
    static bool objectMemberByIndex(Iterator & it, size_t index) { return it.objectMemberByIndex(index); }
    static bool objectMemberByName(Iterator & it, const StringRef & name) { return it.objectMemberByName(name); }
    static bool nextObjectMember(Iterator & it) { return it.nextObjectMember(); }
    static bool nextObjectMember(Iterator & it, StringRef & next_key) { return it.nextObjectMember(next_key); }
    static bool isObjectMember(const Iterator & it) { return it.isObjectMember(); }
    static StringRef getKey(const Iterator & it) { return it.getKey(); }

private:
    rapidjson::Document document;
};

}
#endif
