#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/thread_local_rng.h>
#include <random>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int KEEPER_EXCEPTION;
    }
}

namespace ProfileEvents
{
    extern const Event ZooKeeperUserExceptions;
    extern const Event ZooKeeperHardwareExceptions;
    extern const Event ZooKeeperOtherExceptions;
}


namespace Coordination
{

void Exception::incrementErrorMetrics(Error code_)
{
    if (Coordination::isUserError(code_))
        ProfileEvents::increment(ProfileEvents::ZooKeeperUserExceptions);
    else if (Coordination::isHardwareError(code_))
        ProfileEvents::increment(ProfileEvents::ZooKeeperHardwareExceptions);
    else
        ProfileEvents::increment(ProfileEvents::ZooKeeperOtherExceptions);
}

Exception::Exception(const std::string & msg, Error code_, int)
    : DB::Exception(msg, DB::ErrorCodes::KEEPER_EXCEPTION)
    , code(code_)
{
    incrementErrorMetrics(code);
}

Exception::Exception(PreformattedMessage && msg, Error code_)
    : DB::Exception(std::move(msg), DB::ErrorCodes::KEEPER_EXCEPTION)
    , code(code_)
{
    extendedMessage(errorMessage(code));
    incrementErrorMetrics(code);
}

Exception::Exception(Error code_)
    : Exception(code_, "Coordination error: {}", errorMessage(code_))
{
}

Exception::Exception(const Exception & exc) = default;


SimpleFaultInjection::SimpleFaultInjection(Float64 probability_before, Float64 probability_after_, const String & description_)
{
    if (likely(probability_before == 0.0) && likely(probability_after_ == 0.0))
        return;

    std::bernoulli_distribution fault(probability_before);
    if (fault(thread_local_rng))
        throw Coordination::Exception(Coordination::Error::ZCONNECTIONLOSS, "Fault injected (before {})", description_);

    probability_after = probability_after_;
    description = description_;
    exceptions_level = std::uncaught_exceptions();
}

SimpleFaultInjection::~SimpleFaultInjection() noexcept(false)
{
    if (likely(probability_after == 0.0))
        return;

    /// Do not throw from dtor during unwinding
    if (exceptions_level != std::uncaught_exceptions())
        return;

    std::bernoulli_distribution fault(probability_after);
    if (fault(thread_local_rng))
        throw Coordination::Exception(Coordination::Error::ZCONNECTIONLOSS, "Fault injected (after {})", description);
}

using namespace DB;


static void addRootPath(String & path, const String & root_path)
{
    if (path.empty())
        throw Exception::fromMessage(Error::ZBADARGUMENTS, "Path cannot be empty");

    if (path[0] != '/')
        throw Exception(Error::ZBADARGUMENTS, "Path must begin with /, got path '{}'", path);

    if (root_path.empty())
        return;

    if (path.size() == 1)   /// "/"
        path = root_path;
    else
        path = root_path + path;
}

static void removeRootPath(String & path, const String & root_path)
{
    if (root_path.empty())
        return;

    if (path.size() <= root_path.size())
        throw Exception::fromMessage(Error::ZDATAINCONSISTENCY, "Received path is not longer than root_path");

    path = path.substr(root_path.size());
}


const char * errorMessage(Error code)
{
    switch (code)
    {
        case Error::ZOK:                      return "Ok";
        case Error::ZSYSTEMERROR:             return "System error";
        case Error::ZRUNTIMEINCONSISTENCY:    return "Run time inconsistency";
        case Error::ZDATAINCONSISTENCY:       return "Data inconsistency";
        case Error::ZCONNECTIONLOSS:          return "Connection loss";
        case Error::ZMARSHALLINGERROR:        return "Marshalling error";
        case Error::ZUNIMPLEMENTED:           return "Unimplemented";
        case Error::ZOPERATIONTIMEOUT:        return "Operation timeout";
        case Error::ZBADARGUMENTS:            return "Bad arguments";
        case Error::ZINVALIDSTATE:            return "Invalid zhandle state";
        case Error::ZAPIERROR:                return "API error";
        case Error::ZNONODE:                  return "No node";
        case Error::ZNOAUTH:                  return "Not authenticated";
        case Error::ZBADVERSION:              return "Bad version";
        case Error::ZNOCHILDRENFOREPHEMERALS: return "No children for ephemerals";
        case Error::ZNODEEXISTS:              return "Node exists";
        case Error::ZNOTEMPTY:                return "Not empty";
        case Error::ZSESSIONEXPIRED:          return "Session expired";
        case Error::ZINVALIDCALLBACK:         return "Invalid callback";
        case Error::ZINVALIDACL:              return "Invalid ACL";
        case Error::ZAUTHFAILED:              return "Authentication failed";
        case Error::ZCLOSING:                 return "ZooKeeper is closing";
        case Error::ZNOTHING:                 return "(not error) no server responses to process";
        case Error::ZSESSIONMOVED:            return "Session moved to another server, so operation is ignored";
        case Error::ZNOTREADONLY:             return "State-changing request is passed to read-only server";
    }
}

bool isHardwareError(Error zk_return_code)
{
    return zk_return_code == Error::ZINVALIDSTATE
        || zk_return_code == Error::ZSESSIONEXPIRED
        || zk_return_code == Error::ZSESSIONMOVED
        || zk_return_code == Error::ZCONNECTIONLOSS
        || zk_return_code == Error::ZMARSHALLINGERROR
        || zk_return_code == Error::ZOPERATIONTIMEOUT
        || zk_return_code == Error::ZNOTREADONLY;
}

bool isUserError(Error zk_return_code)
{
    return zk_return_code == Error::ZNONODE
        || zk_return_code == Error::ZBADVERSION
        || zk_return_code == Error::ZNOCHILDRENFOREPHEMERALS
        || zk_return_code == Error::ZNODEEXISTS
        || zk_return_code == Error::ZNOTEMPTY;
}


void CreateRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void RemoveRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void RemoveRecursiveRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void ExistsRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void GetRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void SetRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void ListRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void CheckRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void SetACLRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void GetACLRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void SyncRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }

void CreateResponse::removeRootPath(const String & root_path) { Coordination::removeRootPath(path_created, root_path); }
void WatchResponse::removeRootPath(const String & root_path) { Coordination::removeRootPath(path, root_path); }

void MultiResponse::removeRootPath(const String & root_path)
{
    for (auto & response : responses)
        response->removeRootPath(root_path);
}

}
