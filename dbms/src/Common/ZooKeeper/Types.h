#pragma once
#include <common/Types.h>
#include <future>
#include <memory>
#include <vector>
#include <zookeeper.h>
#include <Poco/Event.h>


namespace zkutil
{

using ACLPtr = const ACL_vector *;
using Stat = ::Stat;
class ZooKeeper;


struct Op
{
public:
    Op() : data(new zoo_op_t) {}
    virtual ~Op() {}

    virtual std::shared_ptr<Op> clone() const = 0;

    virtual std::string getPath() const = 0;

    virtual std::string describe() const = 0;

    std::unique_ptr<zoo_op_t> data;

    struct Remove;
    struct Create;
    struct SetData;
    struct Check;
};

using OpPtr = std::shared_ptr<Op>;


struct Op::Remove : public Op
{
    Remove(const std::string & path_, int32_t version_) :
        path(path_), version(version_)
    {
        zoo_delete_op_init(data.get(), path.c_str(), version);
    }

    OpPtr clone() const override
    {
        return std::make_shared<Remove>(path, version);
    }

    std::string getPath() const override { return path; }

    std::string describe() const override { return "command: remove, path: " + path; }

private:
    std::string path;
    int32_t version;
};

struct Op::Create : public Op
{
    Create(const std::string & path_pattern_, const std::string & value_, ACLPtr acl_, int32_t flags_);

    OpPtr clone() const override
    {
        return std::make_shared<Create>(path_pattern, value, acl, flags);
    }

    std::string getPathCreated() { return created_path.data(); }

    std::string getPath() const override { return path_pattern; }

    std::string describe() const override
    {
        return     "command: create"
                ", path: " + path_pattern +
                ", value: " + value;
    }

private:
    std::string path_pattern;
    std::string value;
    ACLPtr acl;
    int32_t flags;
    std::vector<char> created_path;
};

struct Op::SetData : public Op
{
    SetData(const std::string & path_, const std::string & value_, int32_t version_) :
        path(path_), value(value_), version(version_)
    {
        zoo_set_op_init(data.get(), path.c_str(), value.c_str(), value.size(), version, &stat);
    }

    OpPtr clone() const override
    {
        return std::make_shared<SetData>(path, value, version);
    }

    std::string getPath() const override { return path; }

    std::string describe() const override
    {
        return
            "command: set"
            ", path: " + path +
            ", value: " + value +
            ", version: " + std::to_string(data->set_op.version);
    }

private:
    std::string path;
    std::string value;
    int32_t version;
    Stat stat;
};

struct Op::Check : public Op
{
    Check(const std::string & path_, int32_t version_) :
        path(path_), version(version_)
    {
        zoo_check_op_init(data.get(), path.c_str(), version);
    }

    OpPtr clone() const override
    {
        return std::make_shared<Check>(path, version);
    }

    std::string getPath() const override { return path; }

    std::string describe() const override { return "command: check, path: " + path; }

private:
    std::string path;
    int32_t version;
};

using Ops = std::vector<OpPtr>;


/// C++ version of zoo_op_result_t
struct OpResult
{
    int err;
    std::string value;
    std::unique_ptr<Stat> stat;

    /// ZooKeeper is required for correct chroot path prefixes handling
    explicit OpResult(const zoo_op_result_t & op_result, const ZooKeeper * zookeeper = nullptr);
};
using OpResults = std::vector<OpResult>;
using OpResultsPtr = std::shared_ptr<OpResults>;
using Strings = std::vector<std::string>;


/// Simple structure to handle transaction execution results
struct MultiTransactionInfo
{
    Ops ops;
    int32_t code = ZOK;
    OpResultsPtr op_results;

    MultiTransactionInfo() = default;

    MultiTransactionInfo(int32_t code_, const Ops & ops_, const OpResultsPtr & op_results_)
        : ops(ops_), code(code_), op_results(op_results_) {}

    bool empty() const
    {
        return ops.empty();
    }

    /// Returns failed op if zkutil::isUserError(code) is true
    const Op & getFailedOp() const;
};


namespace CreateMode
{
    extern const int Persistent;
    extern const int Ephemeral;
    extern const int EphemeralSequential;
    extern const int PersistentSequential;
}

using EventPtr = std::shared_ptr<Poco::Event>;

class ZooKeeper;

/// Callback to call when the watch fires.
/// Because callbacks are called in the single "completion" thread internal to libzookeeper,
/// they must execute as quickly as possible (preferably just set some notification).
/// Parameters:
/// zookeeper - zookeeper session to which the fired watch belongs
/// type - event type, one of the *_EVENT constants from zookeeper.h
/// state - session connection state, one of the *_STATE constants from zookeeper.h
/// path - znode path to which the change happened. if event == ZOO_SESSION_EVENT it is either NULL or empty string.
using WatchCallback = std::function<void(ZooKeeper & zookeeper, int type, int state, const char * path)>;


/// Returns first op which code != ZOK or throws an exception
/// ZooKeeper client sets correct OP codes if the transaction fails because of logical (user) errors like ZNODEEXISTS
/// If it is failed because of network error, for example, OP codes is not set.
/// Therefore you should make zkutil::isUserError() check before the function invocation.
size_t getFailedOpIndex(const OpResultsPtr & op_results, int32_t transaction_return_code);

}
