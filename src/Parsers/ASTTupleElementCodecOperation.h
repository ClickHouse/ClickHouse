#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

enum class TupleElementCodecOperationKind : UInt8
{
    Set,
    Remove,
};

/// A CODEC operation attached to one element of the owning ASTTupleDataType.
/// The element is identified by its zero-based position in that Tuple.
class ASTTupleElementCodecOperation final : public IAST
{
public:
    size_t element_index = 0;
    TupleElementCodecOperationKind kind = TupleElementCodecOperationKind::Set;

    const ASTPtr & getCodec() const;
    void validate() const;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(
        WriteBuffer & out,
        const FormatSettings & settings,
        FormatState & state,
        FormatStateStacked frame) const override;
};

}
