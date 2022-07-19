#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/IKeeper.h>


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

Exception::Exception(const std::string & msg, const Error code_, int)
    : DB::Exception(msg, DB::ErrorCodes::KEEPER_EXCEPTION), code(code_)
{
    if (Coordination::isUserError(code))
        ProfileEvents::increment(ProfileEvents::ZooKeeperUserExceptions);
    else if (Coordination::isHardwareError(code))
        ProfileEvents::increment(ProfileEvents::ZooKeeperHardwareExceptions);
    else
        ProfileEvents::increment(ProfileEvents::ZooKeeperOtherExceptions);
}

Exception::Exception(const std::string & msg, const Error code_)
    : Exception(msg + " (" + errorMessage(code_) + ")", code_, 0)
{
}

Exception::Exception(const Error code_)
    : Exception(errorMessage(code_), code_, 0)
{
}

Exception::Exception(const Error code_, const std::string & path)
    : Exception(std::string{errorMessage(code_)} + ", path: " + path, code_, 0)
{
}

Exception::Exception(const Exception & exc) = default;


using namespace DB;


static void addRootPath(String & path, const String & root_path)
{
    if (path.empty())
        throw Exception("Path cannot be empty", Error::ZBADARGUMENTS);

    if (path[0] != '/')
        throw Exception("Path must begin with /, got path '" + path + "'", Error::ZBADARGUMENTS);

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
        throw Exception("Received path is not longer than root_path", Error::ZDATAINCONSISTENCY);

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
    }

    __builtin_unreachable();
}

bool isHardwareError(Error zk_return_code)
{
    return zk_return_code == Error::ZINVALIDSTATE
        || zk_return_code == Error::ZSESSIONEXPIRED
        || zk_return_code == Error::ZSESSIONMOVED
        || zk_return_code == Error::ZCONNECTIONLOSS
        || zk_return_code == Error::ZMARSHALLINGERROR
        || zk_return_code == Error::ZOPERATIONTIMEOUT;
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
void ExistsRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void GetRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void SetRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void ListRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void CheckRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void SetACLRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void GetACLRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void SyncRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }

void MultiRequest::addRootPath(const String & root_path)
{
    for (auto & request : requests)
        request->addRootPath(root_path);
}

void CreateResponse::removeRootPath(const String & root_path) { Coordination::removeRootPath(path_created, root_path); }
void WatchResponse::removeRootPath(const String & root_path) { Coordination::removeRootPath(path, root_path); }

void MultiResponse::removeRootPath(const String & root_path)
{
    for (auto & response : responses)
        response->removeRootPath(root_path);
}

}

