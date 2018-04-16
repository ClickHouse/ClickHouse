#include <DataStreams/FilterBlockInputStream.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class PrewhereFilterBlockInputStream : public FilterBlockInputStream
{
public:
    PrewhereFilterBlockInputStream(const BlockInputStreamPtr & input, const PrewhereInfoPtr & prewhere_info)
            : FilterBlockInputStream(input, prewhere_info->prewhere_actions,
                                     prewhere_info->prewhere_column_name, prewhere_info->remove_prewhere_column)
            , prewhere_info(prewhere_info)
    {
    }

protected:
    Block readImpl() override
    {
        remove_filter = prewhere_info->remove_prewhere_column;
        return FilterBlockInputStream::readImpl();
    }

private:
    PrewhereInfoPtr prewhere_info;
};

}
