#include <string.h>

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

Exception::Exception(const std::string & msg, const int32_t code, int)
    : DB::Exception(msg, DB::ErrorCodes::KEEPER_EXCEPTION), code(code)
{
    if (Coordination::isUserError(code))
        ProfileEvents::increment(ProfileEvents::ZooKeeperUserExceptions);
    else if (Coordination::isHardwareError(code))
        ProfileEvents::increment(ProfileEvents::ZooKeeperHardwareExceptions);
    else
        ProfileEvents::increment(ProfileEvents::ZooKeeperOtherExceptions);
}

Exception::Exception(const std::string & msg, const int32_t code)
    : Exception(msg + " (" + errorMessage(code) + ")", code, 0)
{
}

Exception::Exception(const int32_t code)
    : Exception(errorMessage(code), code, 0)
{
}

Exception::Exception(const int32_t code, const std::string & path)
    : Exception(std::string{errorMessage(code)} + ", path: " + path, code, 0)
{
}

Exception::Exception(const Exception & exc)
    : DB::Exception(exc), code(exc.code)
{
}


using namespace DB;


void addRootPath(String & path, const String & root_path)
{
    if (path.empty())
        throw Exception("Path cannot be empty", ZBADARGUMENTS);

    if (path[0] != '/')
        throw Exception("Path must begin with /", ZBADARGUMENTS);

    if (root_path.empty())
        return;

    if (path.size() == 1)   /// "/"
        path = root_path;
    else
        path = root_path + path;
}

void removeRootPath(String & path, const String & root_path)
{
    if (root_path.empty())
        return;

    if (path.size() <= root_path.size())
        throw Exception("Received path is not longer than root_path", ZDATAINCONSISTENCY);

    path = path.substr(root_path.size());
}


const char * errorMessage(int32_t code)
{
    switch (code)
    {
        case ZOK:                       return "Ok";
        case ZSYSTEMERROR:              return "System error";
        case ZRUNTIMEINCONSISTENCY:     return "Run time inconsistency";
        case ZDATAINCONSISTENCY:        return "Data inconsistency";
        case ZCONNECTIONLOSS:           return "Connection loss";
        case ZMARSHALLINGERROR:         return "Marshalling error";
        case ZUNIMPLEMENTED:            return "Unimplemented";
        case ZOPERATIONTIMEOUT:         return "Operation timeout";
        case ZBADARGUMENTS:             return "Bad arguments";
        case ZINVALIDSTATE:             return "Invalid zhandle state";
        case ZAPIERROR:                 return "API error";
        case ZNONODE:                   return "No node";
        case ZNOAUTH:                   return "Not authenticated";
        case ZBADVERSION:               return "Bad version";
        case ZNOCHILDRENFOREPHEMERALS:  return "No children for ephemerals";
        case ZNODEEXISTS:               return "Node exists";
        case ZNOTEMPTY:                 return "Not empty";
        case ZSESSIONEXPIRED:           return "Session expired";
        case ZINVALIDCALLBACK:          return "Invalid callback";
        case ZINVALIDACL:               return "Invalid ACL";
        case ZAUTHFAILED:               return "Authentication failed";
        case ZCLOSING:                  return "ZooKeeper is closing";
        case ZNOTHING:                  return "(not error) no server responses to process";
        case ZSESSIONMOVED:             return "Session moved to another server, so operation is ignored";
    }
    if (code > 0)
        return strerror(code);

    return "unknown error";
}

bool isHardwareError(int32_t zk_return_code)
{
    return zk_return_code == ZINVALIDSTATE
        || zk_return_code == ZSESSIONEXPIRED
        || zk_return_code == ZSESSIONMOVED
        || zk_return_code == ZCONNECTIONLOSS
        || zk_return_code == ZMARSHALLINGERROR
        || zk_return_code == ZOPERATIONTIMEOUT;
}

bool isUserError(int32_t zk_return_code)
{
    return zk_return_code == ZNONODE
        || zk_return_code == ZBADVERSION
        || zk_return_code == ZNOCHILDRENFOREPHEMERALS
        || zk_return_code == ZNODEEXISTS
        || zk_return_code == ZNOTEMPTY;
}


void CreateRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void RemoveRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void ExistsRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void GetRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void SetRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void ListRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }
void CheckRequest::addRootPath(const String & root_path) { Coordination::addRootPath(path, root_path); }

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

