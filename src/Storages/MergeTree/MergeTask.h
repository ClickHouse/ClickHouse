#include <memory>

namespace DB
{


class BackgroundTask
{
public:
    virtual bool execute() = 0;
    virtual ~BackgroundTask() = default;
};


class MergeTask;

using MergeTaskPtr = std::shared_ptr<MergeTask>;

class MergeTask : public BackgroundTask
{
public:
    bool execute() override;
private:
    virtual void chooseColumns() = 0;
};


class VerticalMergeTask final : public MergeTask
{
private:
    void chooseColumns() override;
};


class HorizontalMergeTask final : public MergeTask
{
private:
    void chooseColumns() override;
};


MergeTaskPtr createMergeTask()
{
    return std::make_shared<VerticalMergeTask>();
}


}
