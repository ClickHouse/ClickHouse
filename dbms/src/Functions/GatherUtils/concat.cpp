#include <Functions/GatherUtils/Selectors.h>
#include <Functions/GatherUtils/Algorithms.h>

namespace DB::GatherUtils
{

struct ArrayConcat : public ArraySinkSourceSelector<ArrayConcat>
{
    using Sources = std::vector<std::unique_ptr<IArraySource>>;

    template <typename Source, typename Sink>
    static void selectSourceSink(Source &&, Sink && sink, const Sources & sources)
    {
        using SourceType = typename std::decay<Source>::type;
        concat<SourceType, Sink>(sources, sink);
    }

    template <typename Source, typename Sink>
    static void selectSourceSink(ConstSource<Source> &&, Sink && sink, const Sources & sources)
    {
        using SourceType = typename std::decay<Source>::type;
        concat<SourceType, Sink>(sources, sink);
    }

    template <typename Source, typename Sink>
    static void selectSourceSink(ConstSource<Source> &, Sink && sink, const Sources & sources)
    {
        using SourceType = typename std::decay<Source>::type;
        concat<SourceType, Sink>(sources, sink);
    }
};

void concat(const std::vector<std::unique_ptr<IArraySource>> & sources, IArraySink & sink)
{
    if (sources.empty())
        throw Exception("Concat function should get at least 1 ArraySource", ErrorCodes::LOGICAL_ERROR);
    return ArrayConcat::select(*sources.front(), sink, sources);
}
}
