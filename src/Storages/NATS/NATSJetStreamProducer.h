#pragma once

#include <Storages/NATS/INATSProducer.h>

namespace DB
{

class NATSJetStreamProducer : public INATSProducer
{
public:
    NATSJetStreamProducer(NATSConnectionPtr connection_, String subject_, std::atomic<bool> & shutdown_called_, LoggerPtr log_);

private:
    natsStatus publishMessage(const String & message) override;

    std::unique_ptr<jsCtx, decltype(&jsCtx_Destroy)> jet_stream_ctx;
    jsOptions jet_stream_options;
};

}
