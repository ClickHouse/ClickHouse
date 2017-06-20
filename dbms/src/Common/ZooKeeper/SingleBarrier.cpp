#include <Common/ZooKeeper/SingleBarrier.h>
#include <Common/ZooKeeper/RWLock.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;
extern const int BARRIER_TIMEOUT;
extern const int NO_SUCH_BARRIER;

}

}

namespace zkutil
{

namespace
{

constexpr long wait_duration = 1000;

}

SingleBarrier::SingleBarrier(GetZooKeeper get_zookeeper_, const std::string & path_, size_t counter_)
    : get_zookeeper{get_zookeeper_}, path{path_}, counter{counter_}
{
    if (!get_zookeeper)
        throw DB::Exception{"No ZooKeeper accessor specified", DB::ErrorCodes::LOGICAL_ERROR};

    auto zookeeper = get_zookeeper();

    Ops ops;
    auto acl = zookeeper->getDefaultACL();

    ops.emplace_back(std::make_unique<zkutil::Op::Create>(path, "", acl, CreateMode::Persistent));

    /// List of tokens.
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(path + "/tokens", "", acl, CreateMode::Persistent));

    /// Tokens are tagged so that we can differentiate obsolete tokens that may
    /// be deleted from those that are currently used to cross this barrier.
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(path + "/tag", "0", acl, CreateMode::Persistent));

    /// We protect some areas of the code in order to reliably determine which
    /// token has unblocked this barrier.
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(path + "/lock", "", acl, CreateMode::Persistent));

    int32_t code = zookeeper->tryMulti(ops);
    if ((code != ZOK) && (code != ZNODEEXISTS))
        throw KeeperException{code};
}

void SingleBarrier::setCancellationHook(CancellationHook cancellation_hook_)
{
    cancellation_hook = cancellation_hook_;
}

void SingleBarrier::enter(UInt64 timeout)
{
    __sync_synchronize();

    bool is_first_crossing = true;

    RWLock lock{get_zookeeper, path + "/lock"};

    try
    {
        lock.acquireWrite();

        auto zookeeper = get_zookeeper();

        auto tag = zookeeper->get(path + "/tag");

        token = tag + "_" + getFQDNOrHostName();

        int32_t code = zookeeper->tryCreate(path + "/tokens/" + token, "", zkutil::CreateMode::Ephemeral);
        if (code == ZNONODE)
            throw DB::Exception{"No such barrier", DB::ErrorCodes::NO_SUCH_BARRIER};
        else if (code == ZNODEEXISTS)
        {
            /// nothing here
        }
        else if (code != ZOK)
            throw KeeperException{code};

        Stopwatch watch;

        while (true)
        {
            auto zookeeper = get_zookeeper();

            auto tokens = zookeeper->getChildren(path + "/tokens", nullptr, event);

            std::sort(tokens.begin(), tokens.end());
            auto it = std::lower_bound(tokens.cbegin(), tokens.cend(), token);

            /// This should never happen.
            if ((it == tokens.cend()) || (*it != token))
                throw DB::Exception{"SingleBarrier: corrupted queue. Own token not found.",
                    DB::ErrorCodes::LOGICAL_ERROR};

            size_t token_count = tokens.size();

            if (is_first_crossing)
            {
                /// Delete all the obsolete tokens.
                for (auto it = tokens.cbegin(); it != tokens.cend(); ++it)
                {
                    const auto & cur_token = *it;

                    size_t pos = cur_token.find('_');
                    if (pos == std::string::npos)
                        throw DB::Exception{"SingleBarrier: corrupted token",
                            DB::ErrorCodes::LOGICAL_ERROR};

                    if (cur_token.compare(0, pos, tag) == 0)
                    {
                        /// All the obsolete tokens have been deleted.
                        break;
                    }

                    (void) zookeeper->tryRemoveEphemeralNodeWithRetries(path + "/tokens/" + cur_token);
                    --token_count;
                }
            }

            if (token_count == counter)
            {
                if (is_first_crossing)
                {
                    /// We are the token that has unblocked this barrier. As such,
                    /// it is our duty to update the tag.
                    UInt64 new_tag = std::stoull(tag) + 1;
                    zookeeper->set(path + "/tag", DB::toString(new_tag));
                }

                lock.release();
                break;
            }

            is_first_crossing = false;

            /// Allow other nodes to progress while we are idling.
            lock.release();

            do
            {
                if ((timeout > 0) && (static_cast<uint32_t>(watch.elapsedSeconds()) > timeout))
                    throw DB::Exception{"SingleBarrier: timeout", DB::ErrorCodes::BARRIER_TIMEOUT};

                abortIfRequested();
            }
            while (!event->tryWait(wait_duration));

            lock.acquireWrite();
        }
    }
    catch (...)
    {
        if (lock.ownsLock())
            lock.release();
        throw;
    }
}

void SingleBarrier::abortIfRequested()
{
    if (cancellation_hook)
    {
        try
        {
            cancellation_hook();
        }
        catch (...)
        {
            try
            {
                /// We have received a cancellation request while trying
                /// to cross the barrier. Therefore we delete our token.
                (void) get_zookeeper()->tryRemoveEphemeralNodeWithRetries(path + "/tokens/" + token);
            }
            catch (...)
            {
                DB::tryLogCurrentException(__PRETTY_FUNCTION__);
            }
            throw;
        }
    }
}

}
