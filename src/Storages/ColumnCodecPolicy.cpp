#include <Storages/ColumnCodecPolicy.h>

#include <utility>

namespace DB
{

ColumnCodecPolicyView::ColumnCodecPolicyView(
    ASTPtr root_codec_,
    const TupleElementCodecs * element_codecs_)
    : root_codec(std::move(root_codec_))
    , element_codecs(element_codecs_)
{
}

bool ColumnCodecPolicyView::hasElements() const
{
    return element_codecs && !element_codecs->empty();
}

std::optional<ColumnCodecPolicyView::Declaration> ColumnCodecPolicyView::find(const TupleCodecPath & stream_path) const
{
    if (element_codecs)
    {
        auto prefix = stream_path;
        while (!prefix.empty())
        {
            if (const auto it = element_codecs->find(prefix); it != element_codecs->end())
                return Declaration{it->second, std::move(prefix)};
            prefix.pop_back();
        }
    }

    if (root_codec)
        return Declaration{root_codec, {}};

    return std::nullopt;
}

}
