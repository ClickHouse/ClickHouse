#pragma once

#include <Parsers/ASTExpressionList.h>

namespace Poco::JSON { class Object; }

namespace DB
{

class JSONObjectReader;
class JSONObjectWriter;

/// AST for data types, e.g. UInt8 or Tuple(x UInt8, y Enum(a = 1))
class ASTDataType : public IAST
{
    struct ASTDataTypeFlags
    {
        using ParentFlags = void;
        static constexpr UInt32 RESERVED_BITS = 1;

        UInt32 remove_codec : 1;
        UInt32 unused : 31;
    };

public:
    String name;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr getArguments() const;
    void resetArguments();

    /// Codec operation attached to this type when it is used as a Tuple element.
    ASTPtr getCodec() const;
    bool hasCodec() const { return static_cast<bool>(getCodec()); }
    bool hasCodecRemoval() const { return flags<ASTDataTypeFlags>().remove_codec; }
    void setCodec(ASTPtr codec);
    void setCodecRemoval(bool value = true);
    void resetCodecOperation();

protected:
    void cloneDataTypeChildrenTo(ASTDataType & target) const;
    void writeCodecJSON(JSONObjectWriter & writer) const;
    void readCodecJSON(JSONObjectReader & reader);
    void updateCodecHash(SipHash & hash_state) const;
    void formatCodecOperation(
        WriteBuffer & ostr,
        const FormatSettings & settings,
        FormatState & state,
        FormatStateStacked frame) const;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

template <typename... Args>
boost::intrusive_ptr<ASTDataType> makeASTDataType(const String & name, Args &&... args)
{
    auto data_type = make_intrusive<ASTDataType>();
    data_type->name = name;

    if constexpr (sizeof...(args))
    {
        auto arguments = make_intrusive<ASTExpressionList>();
        data_type->children.push_back(arguments);
        arguments->children = {std::forward<Args>(args)...};
    }

    return data_type;
}

}
