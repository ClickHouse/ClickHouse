#include <Storages/Kafka/Clients/Protocol/Messages/FindCoordinator.h>

namespace Kafka::Protocol
{

void FindCoordinatorRequest::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, min_version, max_version, "FindCoordinator");
    if (version >= 4)
    {
        w.writeInt8(key_type);
        w.writeCompactArray(coordinator_keys, [](Writer & wr, const String & key) { wr.writeCompactString(key); });
    }
    else
    {
        /// v3: a single key, then the key type.
        if (coordinator_keys.size() != 1)
            throw Exception(ErrorKind::Protocol, "FindCoordinator v3 carries exactly one key, got {}", coordinator_keys.size());
        w.writeCompactString(coordinator_keys.front());
        w.writeInt8(key_type);
    }
    w.writeEmptyTaggedFields();
}

FindCoordinatorRequest FindCoordinatorRequest::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, min_version, max_version, "FindCoordinator");
    FindCoordinatorRequest req;
    if (version >= 4)
    {
        req.key_type = r.readInt8();
        req.coordinator_keys = r.readCompactArray<String>([](Reader & rd) { return rd.readCompactString(); });
    }
    else
    {
        req.coordinator_keys = {r.readCompactString()};
        req.key_type = r.readInt8();
    }
    r.skipTaggedFields();
    return req;
}

void FindCoordinatorResponse::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, FindCoordinatorRequest::min_version, FindCoordinatorRequest::max_version, "FindCoordinator");
    w.writeInt32(throttle_time_ms);
    if (version >= 4)
    {
        w.writeCompactArray(coordinators, [](Writer & wr, const Coordinator & coordinator)
        {
            wr.writeCompactString(coordinator.key);
            wr.writeInt32(coordinator.node_id);
            wr.writeCompactString(coordinator.host);
            wr.writeInt32(coordinator.port);
            wr.writeInt16(coordinator.error_code);
            wr.writeCompactNullableString(coordinator.error_message);
            wr.writeEmptyTaggedFields();
        });
    }
    else
    {
        if (coordinators.size() != 1)
            throw Exception(ErrorKind::Protocol, "FindCoordinator v3 carries exactly one coordinator, got {}", coordinators.size());
        const auto & coordinator = coordinators.front();
        w.writeInt16(coordinator.error_code);
        w.writeCompactNullableString(coordinator.error_message);
        w.writeInt32(coordinator.node_id);
        w.writeCompactString(coordinator.host);
        w.writeInt32(coordinator.port);
    }
    w.writeEmptyTaggedFields();
}

FindCoordinatorResponse FindCoordinatorResponse::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, FindCoordinatorRequest::min_version, FindCoordinatorRequest::max_version, "FindCoordinator");
    FindCoordinatorResponse resp;
    resp.throttle_time_ms = r.readInt32();
    if (version >= 4)
    {
        resp.coordinators = r.readCompactArray<Coordinator>([](Reader & rd)
        {
            Coordinator coordinator;
            coordinator.key = rd.readCompactString();
            coordinator.node_id = rd.readInt32();
            coordinator.host = rd.readCompactString();
            coordinator.port = rd.readInt32();
            coordinator.error_code = rd.readInt16();
            coordinator.error_message = rd.readCompactNullableString();
            rd.skipTaggedFields();
            return coordinator;
        });
    }
    else
    {
        /// v3: flat single coordinator; the key is not echoed back.
        Coordinator coordinator;
        coordinator.error_code = r.readInt16();
        coordinator.error_message = r.readCompactNullableString();
        coordinator.node_id = r.readInt32();
        coordinator.host = r.readCompactString();
        coordinator.port = r.readInt32();
        resp.coordinators = {std::move(coordinator)};
    }
    r.skipTaggedFields();
    return resp;
}

}
