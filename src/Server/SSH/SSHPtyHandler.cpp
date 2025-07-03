#include <Server/SSH/SSHPtyHandler.h>

#if USE_SSH && defined(OS_LINUX)

#include <Access/Common/AuthenticationType.h>
#include <Access/Credentials.h>
#include <Access/SSH/SSHPublicKey.h>
#include <Common/clibssh.h>
#include <Common/logger_useful.h>
#include <Core/Names.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Pipe.h>
#include <Server/ClientEmbedded/ClientEmbeddedRunner.h>
#include <Server/ClientEmbedded/IClientDescriptorSet.h>
#include <Server/ClientEmbedded/PtyClientDescriptorSet.h>
#include <Server/SSH/SSHChannel.h>
#include <Server/SSH/SSHEvent.h>

#if defined(USE_MUSL)
#   include <poll.h>
#else
#   include <sys/poll.h>
#endif

#include <atomic>
#include <stdexcept>

#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>

namespace
{

/*
Need to generate adapter functions, such for each member function, for example:

class SessionCallback
{
For this:
    ssh_channel channelOpen(ssh_session session)
    {
        channel = SSHChannel(session);
        return channel->get();
    }


Generate this:
    static ssh_channel channelOpenAdapter(ssh_session session, void * userdata)
    {
        auto * self = static_cast<SessionCallback*>(userdata);
        return self->channel_open;
    }
}

We just static cast userdata to our class and then call member function.
This is needed to use c++ classes in libssh callbacks.
Maybe there is a better way? Or just write boilerplate code and avoid macros?
*/
#define GENERATE_ADAPTER_FUNCTION(class, func_name, return_type) \
    template <typename... Args> \
    static return_type func_name##Adapter(Args... args, void * userdata) \
    { \
        auto * self = static_cast<class *>(userdata); \
        return self->func_name(args...); \
    }
}

namespace DB
{

namespace
{


// Wrapper around ssh_channel_callbacks. Each callback must not throw any exceptions, as c code is executed
class ChannelCallback
{
public:
    using DescriptorSet = IClientDescriptorSet::DescriptorSet;

    explicit ChannelCallback
    (
        ::ssh::SSHChannel && channel_,
        std::unique_ptr<Session> && dbSession_,
        const SSHPtyHandler::Options & options_
    )
        : channel(std::move(channel_))
        , db_session(std::move(dbSession_))
        , log(&Poco::Logger::get("SSHChannelCallback"))
        , enable_client_options_passing(options_.enable_client_options_passing)
    {
        channel_cb.userdata = this;
        channel_cb.channel_pty_request_function = ptyRequestAdapter<ssh_session, ssh_channel, const char *, int, int, int, int>;
        channel_cb.channel_shell_request_function = shellRequestAdapter<ssh_session, ssh_channel>;
        channel_cb.channel_data_function = dataFunctionAdapter<ssh_session, ssh_channel, void *, uint32_t, int>;
        channel_cb.channel_eof_function = eofFunctionAdapter<ssh_session, ssh_channel>;
        channel_cb.channel_pty_window_change_function = ptyResizeAdapter<ssh_session, ssh_channel, int, int, int, int>;
        channel_cb.channel_env_request_function = envRequestAdapter<ssh_session, ssh_channel, const char *, const char*>;
        channel_cb.channel_exec_request_function = execRequestAdapter<ssh_session, ssh_channel, const char *>;
        channel_cb.channel_subsystem_request_function = subsystemRequestAdapter<ssh_session, ssh_channel, const char *>;
        ssh_callbacks_init(&channel_cb) ssh_set_channel_callbacks(channel.getCChannelPtr(), &channel_cb);
    }

    bool hasClientFinished() { return client_runner.has_value() && client_runner->hasFinished(); }
    int getClientExitCode() { return client_runner.has_value() && client_runner->getExitCode(); }


    DescriptorSet client_input_output;
    ::ssh::SSHChannel channel;
    std::unique_ptr<Session> db_session;
    std::optional<ClientEmbeddedRunner> client_runner;
    Poco::Logger * log;

    /// The functionality to pass options
    const bool enable_client_options_passing;
    NameToNameMap env;

private:
    int ptyRequest(ssh_session, ssh_channel, const char * term, int width, int height, int width_pixels, int height_pixels) noexcept
    {
        LOG_TRACE(log, "Received pty request");
        if (!db_session || client_runner.has_value())
            return SSH_ERROR;
        try
        {
            auto client_descriptors = std::make_unique<PtyClientDescriptorSet>(String(term), width, height, width_pixels, height_pixels);
            client_runner.emplace(std::move(client_descriptors), std::move(db_session));
        }
        catch (...)
        {
            tryLogCurrentException(log, "Exception from creating pty");
            return SSH_ERROR;
        }

        return SSH_OK;
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, ptyRequest, int)

    int ptyResize(ssh_session, ssh_channel, int width, int height, int width_pixels, int height_pixels) noexcept
    {
        LOG_TRACE(log, "Received pty resize");
        if (!client_runner.has_value() || !client_runner->hasPty())
        {
            return SSH_ERROR;
        }

        try
        {
            client_runner->changeWindowSize(width, height, width_pixels, height_pixels);
            return SSH_OK;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Exception from changing window size");
            return SSH_ERROR;
        }
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, ptyResize, int)

    int dataFunction(ssh_session, ssh_channel, void * data, uint32_t len, int /*is_stderr*/) const noexcept
    {
        if (len == 0 || client_input_output.in == -1)
        {
            return 0;
        }

        return static_cast<int>(write(client_input_output.in, data, len));
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, dataFunction, int)

    void eofFunction(ssh_session, ssh_channel) const noexcept
    {
        if (!client_runner.has_value())
            return;

        client_runner->closeStdIn();
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, eofFunction, void)

    int subsystemRequest(ssh_session, ssh_channel, const char * subsystem) noexcept
    {
        LOG_TRACE(log, "Received subsystem request");
        if (strcmp(subsystem, "ch-client") != 0)
        {
            return SSH_ERROR;
        }
        LOG_TRACE(log, "Subsystem is supported");
        if (!client_runner.has_value() || client_runner->hasStarted() || !client_runner->hasPty())
        {
            return SSH_ERROR;
        }

        try
        {
            client_runner->run(env);
            client_input_output = client_runner->getDescriptorsForServer();
            return SSH_OK;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Exception from starting client");
            return SSH_ERROR;
        }
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, subsystemRequest, int)

    int shellRequest(ssh_session, ssh_channel) noexcept
    {
        LOG_TRACE(log, "Received shell request");
        if (!client_runner.has_value() || client_runner->hasStarted() || !client_runner->hasPty())
        {
            return SSH_ERROR;
        }

        try
        {
            client_runner->run(env);
            client_input_output = client_runner->getDescriptorsForServer();
            return SSH_OK;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Exception from starting client");
            return SSH_ERROR;
        }
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, shellRequest, int)

    int envRequest(ssh_session, ssh_channel, const char * env_name, const char * env_value)
    {
        LOG_TEST(log, "Received env request. Client options passing is {}enabled", enable_client_options_passing ? "" : "not ");
        if (enable_client_options_passing)
            env[env_name] = env_value;
        return SSH_OK;
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, envRequest, int)

    int execNopty(const String & command)
    {
        if (db_session)
        {
            try
            {
                auto client_descriptors = std::make_unique<PipeClientDescriptorSet>();
                client_runner.emplace(std::move(client_descriptors), std::move(db_session));
                client_runner->run(env, command);
                client_input_output = client_runner->getDescriptorsForServer();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Exception from starting client with no pty");
                return SSH_ERROR;
            }
        }
        return SSH_OK;
    }

    int execRequest(ssh_session, ssh_channel, const char * command)
    {
        if (client_runner.has_value() && (client_runner->hasStarted() || !client_runner->hasPty()))
        {
            return SSH_ERROR;
        }
        if (client_runner.has_value())
        {
            try
            {
                client_runner->run(env, command);
                client_input_output = client_runner->getDescriptorsForServer();
                return SSH_OK;
            }
            catch (...)
            {
                tryLogCurrentException(log, "Exception from starting client with pre entered query");
                return SSH_ERROR;
            }
        }
        return execNopty(String(command));
    }

    GENERATE_ADAPTER_FUNCTION(ChannelCallback, execRequest, int)


    ssh_channel_callbacks_struct channel_cb = {};
};

int process_stdout(socket_t fd, int revents, void * userdata)
{
    char buf[1024];
    int n = -1;
    ssh_channel channel = static_cast<ssh_channel>(userdata);

    if (channel != nullptr && (revents & POLLIN) != 0)
    {
        n = static_cast<int>(read(fd, buf, 1024));
        if (n > 0)
        {
            ssh_channel_write(channel, buf, n);
        }
    }

    return n;
}

int process_stderr(socket_t fd, int revents, void * userdata)
{
    char buf[1024];
    int n = -1;
    ssh_channel channel = static_cast<ssh_channel>(userdata);

    if (channel != nullptr && (revents & POLLIN) != 0)
    {
        n = static_cast<int>(read(fd, buf, 1024));
        if (n > 0)
        {
            ssh_channel_write_stderr(channel, buf, n);
        }
    }

    return n;
}

// Wrapper around ssh_server_callbacks. Each callback must not throw any exceptions, as c code is executed
class SessionCallback
{
public:
    explicit SessionCallback
    (
        ::ssh::SSHSession & session,
        IServer & server,
        const Poco::Net::SocketAddress & address_,
        const SSHPtyHandler::Options & options_
    )
        : server_context(server.context())
        , peer_address(address_)
        , log(&Poco::Logger::get("SSHSessionCallback"))
        , options(options_)
    {
        server_cb.userdata = this;
        server_cb.auth_pubkey_function = authPublickeyAdapter<ssh_session, const char *, ssh_key, char>;
        server_cb.auth_password_function = authPasswordAdapter<ssh_session, const char *, const char *>;
        ssh_set_auth_methods(session.getInternalPtr(), SSH_AUTH_METHOD_PASSWORD | SSH_AUTH_METHOD_PUBLICKEY);
        server_cb.channel_open_request_session_function = channelOpenAdapter<ssh_session>;

        ssh_callbacks_init(&server_cb)
        ssh_set_server_callbacks(session.getInternalPtr(), &server_cb);
    }

    size_t auth_attempts = 0;
    bool authenticated = false;
    std::unique_ptr<Session> db_session;
    DB::ContextMutablePtr server_context;
    Poco::Net::SocketAddress peer_address;
    std::unique_ptr<ChannelCallback> channel_callback;
    Poco::Logger * log;
    const SSHPtyHandler::Options options;

    ssh_channel channelOpen(ssh_session session) noexcept
    {
        LOG_DEBUG(log, "Opening a channel");
        if (!db_session)
        {
            return nullptr;
        }
        try
        {
            auto channel = ::ssh::SSHChannel(session);
            channel_callback = std::make_unique<ChannelCallback>(std::move(channel), std::move(db_session), options);
            return channel_callback->channel.getCChannelPtr();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Error while opening channel:");
            return nullptr;
        }
    }

    GENERATE_ADAPTER_FUNCTION(SessionCallback, channelOpen, ssh_channel)

    int authPublickey(ssh_session, const char * user, ssh_key key, char signature_state) noexcept
    {
        try
        {
            LOG_TRACE(log, "Authenticating with public key");
            auto db_session_created = std::make_unique<Session>(server_context, ClientInfo::Interface::LOCAL);
            String user_name(user);

            if (signature_state == SSH_PUBLICKEY_STATE_NONE)
            {
                auto user_has_ssh_auth_type = [](auto user_authentication_type) { return user_authentication_type == AuthenticationType::SSH_KEY; };
                auto user_auth_types = db_session_created->getAuthenticationTypes(user_name);

                /// User {} doesn't have SSH_KEY authentication type, so we will try to authenticate it using a password.
                if (auto result = std::ranges::find_if(user_auth_types, user_has_ssh_auth_type); result == user_auth_types.end())
                    return SSH_AUTH_PARTIAL;

                return SSH_AUTH_SUCCESS;
            }

            if (signature_state != SSH_PUBLICKEY_STATE_VALID)
            {
                ++auth_attempts;
                return SSH_AUTH_DENIED;
            }

            SSHKey wrapped_key(key);
            /// Workaround not to deallocate the key that will be used further.
            wrapped_key.setNeedsDeallocation(false);
            /// The signature is checked, so just verify that user is associated with publickey.
            /// For reference: the OpenSSH's server does roughly the same:
            /// https://github.com/openssh/openssh-portable/blob/826483d51a9fee60703298bbf839d9ce37943474/auth2-pubkey.c#L226-L234
            db_session_created->authenticate(SSHPTYCredentials{user_name, wrapped_key}, peer_address);

            authenticated = true;
            db_session = std::move(db_session_created);
            return SSH_AUTH_SUCCESS;
        }
        catch (...)
        {
            tryLogCurrentException(log);
            ++auth_attempts;
            return SSH_AUTH_DENIED;
        }
    }

    GENERATE_ADAPTER_FUNCTION(SessionCallback, authPublickey, int)

    int authPassword(ssh_session, const char * user, const char * password)
    {
        try
        {
            LOG_TRACE(log, "Authenticating with password");
            auto db_session_created = std::make_unique<Session>(server_context, ClientInfo::Interface::LOCAL);
            db_session_created->authenticate(BasicCredentials{user, password}, peer_address);
            authenticated = true;
            db_session = std::move(db_session_created);
            return SSH_AUTH_SUCCESS;
        }
        catch (...)
        {
            tryLogCurrentException(log);
            ++auth_attempts;
            return SSH_AUTH_DENIED;
        }
    }

    GENERATE_ADAPTER_FUNCTION(SessionCallback, authPassword, int)

    ssh_server_callbacks_struct server_cb = {};
};

}

SSHPtyHandler::SSHPtyHandler(
    IServer & server_,
    ::ssh::SSHSession session_,
    const Poco::Net::StreamSocket & socket_,
    const Options & options_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , log(&Poco::Logger::get("SSHPtyHandler"))
    , session(std::move(session_))
    , options(options_)
{
}

SSHPtyHandler::~SSHPtyHandler()
{
    session.disconnect();
}

void SSHPtyHandler::run()
{
    ::ssh::SSHEvent event;
    SessionCallback sdata(session, server, socket().peerAddress(), options);
    session.handleKeyExchange();
    event.addSession(session);
    int max_iterations = options.auth_timeout_seconds * 1000 / options.event_poll_interval_milliseconds;
    int n = 0;
    while (!sdata.authenticated || !sdata.channel_callback)
    {
        /* If the user has used up all attempts, or if he hasn't been able to
         * authenticate in auth_timeout_seconds, disconnect. */
        if (sdata.auth_attempts >= options.max_auth_attempts || n >= max_iterations)
            return;

        if (server.isCancelled())
            return;

        event.poll(options.event_poll_interval_milliseconds);
        n++;
    }
    bool fds_set = false;

    do
    {
        /* Poll the main event which takes care of the session, the channel and
         * even our client's stdout/stderr (once it's started). */
        event.poll(options.event_poll_interval_milliseconds);

        /* If client's stdout/stderr has been registered with the event,
         * or the client hasn't started yet, continue. */
        if (fds_set || sdata.channel_callback->client_input_output.out == -1)
            continue;

        /* Executed only once, once the client starts. */
        fds_set = true;

        /* If stdout valid, add stdout to be monitored by the poll event. */
        if (sdata.channel_callback->client_input_output.out != -1)
            event.addFd(sdata.channel_callback->client_input_output.out, POLLIN, process_stdout, sdata.channel_callback->channel.getCChannelPtr());

        if (sdata.channel_callback->client_input_output.err != -1)
            event.addFd(sdata.channel_callback->client_input_output.err, POLLIN, process_stderr, sdata.channel_callback->channel.getCChannelPtr());

    }
    while (sdata.channel_callback->channel.isOpen() && !sdata.channel_callback->hasClientFinished() && !server.isCancelled());

    LOG_DEBUG(
        log,
        "Finishing connection with state: channel open: {}, embedded client finished: {}, server cancelled: {}",
        sdata.channel_callback->channel.isOpen(), sdata.channel_callback->hasClientFinished(), server.isCancelled()
    );

    event.removeFd(sdata.channel_callback->client_input_output.out);
    event.removeFd(sdata.channel_callback->client_input_output.err);


    sdata.channel_callback->channel.sendEof();
    sdata.channel_callback->channel.sendExitStatus(sdata.channel_callback->getClientExitCode());
    sdata.channel_callback->channel.close();

    /* Wait up to finish_timeout_seconds seconds for the client to terminate the session. */
    max_iterations = options.finish_timeout_seconds * 1000 / options.event_poll_interval_milliseconds;
    for (n = 0; n < max_iterations && !session.hasFinished(); n++)
        event.poll(options.event_poll_interval_milliseconds);

    LOG_DEBUG(log, "Connection closed");
}

}

#endif
