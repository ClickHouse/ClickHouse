#include <Compression/CompressionCodecFirstApplicable.h>

#include <Parsers/IAST.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

CompressionCodecFirstApplicable::CompressionCodecFirstApplicable(ASTPtr description_, CompressionCodecPtr selected_)
    : description(std::move(description_))
    , selected(std::move(selected_))
{
    if (!description || !selected)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompressionCodecFirstApplicable requires a description and a selected codec");
}

ASTPtr CompressionCodecFirstApplicable::getCodecDescription() const
{
    return description->clone();
}

void CompressionCodecFirstApplicable::updateHash(SipHash & hash) const
{
    /// The description determines both the selection without a type and the selections with a type.
    hash.update(description->formatWithSecretsOneLine());
}

}
