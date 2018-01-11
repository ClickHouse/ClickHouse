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

struct Op
{
public:
    Op() : data(new zoo_op_t) {}
    virtual ~Op() {}

    virtual std::unique_ptr<Op> clone() const = 0;

    virtual std::string describe() = 0;

    std::unique_ptr<zoo_op_t> data;

    struct Remove;
    struct Create;
    struct SetData;
    struct Check;
};

struct Op::Remove : public Op
{
    Remove(const std::string & path_, int32_t version_) :
        path(path_), version(version_)
    {
        zoo_delete_op_init(data.get(), path.c_str(), version);
    }

    std::unique_ptr<Op> clone() const override
    {
        return std::unique_ptr<zkutil::Op>(new Remove(path, version));
    }

    std::string describe() override { return "command: remove, path: " + path; }

private:
    std::string path;
    int32_t version;
};

struct Op::Create : public Op
{
    Create(const std::string & path_, const std::string & value_, ACLPtr acl_, int32_t flags_);

    std::unique_ptr<Op> clone() const override
    {
        return std::unique_ptr<zkutil::Op>(new Create(path, value, acl, flags));
    }

    std::string getPathCreated()
    {
        return created_path.data();
    }

    std::string describe() override
    {
        return     "command: create"
                ", path: " + path +
                ", value: " + value;
    }

private:
    std::string path;
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

    std::unique_ptr<Op> clone() const override
    {
        return std::unique_ptr<zkutil::Op>(new SetData(path, value, version));
    }

    std::string describe() override
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

    std::unique_ptr<Op> clone() const override
    {
        return std::unique_ptr<zkutil::Op>(new Check(path, version));
    }

    std::string describe() override { return "command: check, path: " + path; }

private:
    std::string path;
    int32_t version;
};

struct OpResult : public zoo_op_result_t
{
    /// Pointers in this class point to fields of class Op.
    /// Op instances have the same (or longer lifetime), therefore destructor is not required.
};

using OpPtr = std::unique_ptr<Op>;
using Ops = std::vector<OpPtr>;
using OpResults = std::vector<OpResult>;
using OpResultsPtr = std::shared_ptr<OpResults>;
using Strings = std::vector<std::string>;

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

}
