#include <Access/GSSAcceptor.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>

#include <Poco/StringTokenizer.h>

#include <mutex>
#include <tuple>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
    extern const int KERBEROS_ERROR;
}

GSSAcceptorContext::GSSAcceptorContext(const GSSAcceptorContext::Params& params_)
    : params(params_)
{
}

GSSAcceptorContext::~GSSAcceptorContext()
{
    resetHandles();
}

const String & GSSAcceptorContext::getRealm() const
{
    if (!isReady())
        throwNotReady();
    return realm;
}

bool GSSAcceptorContext::isFailed() const
{
    return is_failed;
}

#if USE_KRB5

namespace
{

std::recursive_mutex gss_global_mutex;

struct PrincipalName
{
    explicit PrincipalName(String principal);
//  operator String() const;

    String name;
    std::vector<String> instances;
    String realm;
};

PrincipalName::PrincipalName(String principal)
{
    const auto at_pos = principal.find('@');
    if (at_pos != std::string::npos)
    {
        realm = principal.substr(at_pos + 1);
        principal.resize(at_pos);
    }

    Poco::StringTokenizer st(principal, "/");
    auto it = st.begin();
    if (it != st.end())
    {
        name = *it;
        instances.assign(++it, st.end());
    }
}

/*
PrincipalName::operator String() const
{
    String principal = name;

    for (const auto & instance : instances)
    {
        principal += '/';
        principal += instance;
    }

    principal += '@';
    principal += realm;

    return principal;
}
*/

String bufferToString(const gss_buffer_desc & buf)
{
    String str;

    if (buf.length > 0 && buf.value != nullptr)
    {
        str.assign(static_cast<char *>(buf.value), buf.length);
        while (!str.empty() && str.back() == '\0') { str.pop_back(); }
    }

    return str;
}

String extractSpecificStatusMessages(OM_uint32 status_code, int status_type, const gss_OID & mech_type)
{
    std::scoped_lock lock(gss_global_mutex);

    String messages;
    OM_uint32 message_context = 0;

    do
    {
        gss_buffer_desc status_string_buf;
        status_string_buf.length = 0;
        status_string_buf.value = nullptr;

        SCOPE_EXIT({
            OM_uint32 minor_status = 0;
            [[maybe_unused]] OM_uint32 major_status = gss_release_buffer(
                &minor_status,
                &status_string_buf
            );
        });

        OM_uint32 minor_status = 0;
        [[maybe_unused]] OM_uint32 major_status = gss_display_status(
            &minor_status,
            status_code,
            status_type,
            mech_type,
            &message_context,
            &status_string_buf
        );

        const auto message = bufferToString(status_string_buf);

        if (!message.empty())
        {
            if (!messages.empty())
                messages += ", ";

            messages += message;
        }
    } while (message_context != 0);

    return messages;
}

String extractStatusMessages(OM_uint32 major_status_code, OM_uint32 minor_status_code, const gss_OID & mech_type)
{
    std::scoped_lock lock(gss_global_mutex);

    const auto gss_messages = extractSpecificStatusMessages(major_status_code, GSS_C_GSS_CODE, mech_type);
    const auto mech_messages = extractSpecificStatusMessages(minor_status_code, GSS_C_MECH_CODE, mech_type);

    String messages;

    if (!gss_messages.empty())
        messages += "Majors: " + gss_messages;

    if (!mech_messages.empty())
    {
        if (!messages.empty())
            messages += "; ";

        messages += "Minors: " + mech_messages;
    }

    return messages;
}

std::pair<String, String> extractNameAndRealm(const gss_name_t & name)
{
    std::scoped_lock lock(gss_global_mutex);

    gss_buffer_desc name_buf;
    name_buf.length = 0;
    name_buf.value = nullptr;

    SCOPE_EXIT({
        OM_uint32 minor_status = 0;
        [[maybe_unused]] OM_uint32 major_status = gss_release_buffer(
            &minor_status,
            &name_buf
        );
    });

    OM_uint32 minor_status = 0;
    [[maybe_unused]] OM_uint32 major_status = gss_display_name(
        &minor_status,
        name,
        &name_buf,
        nullptr
    );

    const PrincipalName principal(bufferToString(name_buf));
    return { principal.name, principal.realm };
}

bool equalMechanisms(const String & left_str, const gss_OID & right_oid)
{
    std::scoped_lock lock(gss_global_mutex);

    gss_buffer_desc left_buf;
    left_buf.length = left_str.size();
    left_buf.value = const_cast<char *>(left_str.c_str());

    gss_OID left_oid = GSS_C_NO_OID;

    SCOPE_EXIT({
        if (left_oid != GSS_C_NO_OID)
        {
            OM_uint32 minor_status = 0;
            [[maybe_unused]] OM_uint32 major_status = gss_release_oid(
                &minor_status,
                &left_oid
            );
            left_oid = GSS_C_NO_OID;
        }
    });

    OM_uint32 minor_status = 0;
    OM_uint32 major_status = gss_str_to_oid(
        &minor_status,
        &left_buf,
        &left_oid
    );

    if (GSS_ERROR(major_status))
        return false;

    return gss_oid_equal(left_oid, right_oid);
}

}

void GSSAcceptorContext::reset()
{
    is_ready = false;
    is_failed = false;
    user_name.clear();
    realm.clear();
    initHandles();
}

void GSSAcceptorContext::resetHandles() noexcept
{
    std::scoped_lock lock(gss_global_mutex);

    if (acceptor_credentials_handle != GSS_C_NO_CREDENTIAL)
    {
        OM_uint32 minor_status = 0;
        [[maybe_unused]] OM_uint32 major_status = gss_release_cred(
            &minor_status,
            &acceptor_credentials_handle
        );
        acceptor_credentials_handle = GSS_C_NO_CREDENTIAL;
    }

    if (context_handle != GSS_C_NO_CONTEXT)
    {
        OM_uint32 minor_status = 0;
        [[maybe_unused]] OM_uint32 major_status = gss_delete_sec_context(
            &minor_status,
            &context_handle,
            GSS_C_NO_BUFFER
        );
        context_handle = GSS_C_NO_CONTEXT;
    }
}

void GSSAcceptorContext::initHandles()
{
    std::scoped_lock lock(gss_global_mutex);

    resetHandles();

    if (!params.principal.empty())
    {
        if (!params.realm.empty())
            throw Exception("Realm and principal name cannot be specified simultaneously", ErrorCodes::BAD_ARGUMENTS);

        gss_buffer_desc acceptor_name_buf;
        acceptor_name_buf.length = params.principal.size();
        acceptor_name_buf.value = const_cast<char *>(params.principal.c_str());

        gss_name_t acceptor_name = GSS_C_NO_NAME;

        SCOPE_EXIT({
            if (acceptor_name != GSS_C_NO_NAME)
            {
                OM_uint32 minor_status = 0;
                [[maybe_unused]] OM_uint32 major_status = gss_release_name(
                    &minor_status,
                    &acceptor_name
                );
                acceptor_name = GSS_C_NO_NAME;
            }
        });

        OM_uint32 minor_status = 0;
        OM_uint32 major_status = gss_import_name(
            &minor_status,
            &acceptor_name_buf,
            GSS_C_NT_HOSTBASED_SERVICE,
            &acceptor_name
        );

        if (GSS_ERROR(major_status))
        {
            const auto messages = extractStatusMessages(major_status, minor_status, GSS_C_NO_OID);
            throw Exception("gss_import_name() failed" + (messages.empty() ? "" : ": " + messages), ErrorCodes::KERBEROS_ERROR);
        }

        minor_status = 0;
        major_status = gss_acquire_cred(
            &minor_status,
            acceptor_name,
            GSS_C_INDEFINITE,
            GSS_C_NO_OID_SET,
            GSS_C_ACCEPT,
            &acceptor_credentials_handle,
            nullptr,
            nullptr
        );

        if (GSS_ERROR(major_status))
        {
            const auto messages = extractStatusMessages(major_status, minor_status, GSS_C_NO_OID);
            throw Exception("gss_acquire_cred() failed" + (messages.empty() ? "" : ": " + messages), ErrorCodes::KERBEROS_ERROR);
        }
    }
}

String GSSAcceptorContext::processToken(const String & input_token, Poco::Logger * log)
{
    std::scoped_lock lock(gss_global_mutex);

    String output_token;

    try
    {
        if (is_ready || is_failed || context_handle == GSS_C_NO_CONTEXT)
            reset();

        gss_buffer_desc input_token_buf;
        input_token_buf.length = input_token.size();
        input_token_buf.value = const_cast<char *>(input_token.c_str());

        gss_buffer_desc output_token_buf;
        output_token_buf.length = 0;
        output_token_buf.value = nullptr;

        gss_name_t initiator_name = GSS_C_NO_NAME;
        gss_OID mech_type = GSS_C_NO_OID;
        OM_uint32 flags = 0;

        SCOPE_EXIT({
            if (initiator_name != GSS_C_NO_NAME)
            {
                OM_uint32 minor_status = 0;
                [[maybe_unused]] OM_uint32 major_status = gss_release_name(
                    &minor_status,
                    &initiator_name
                );
                initiator_name = GSS_C_NO_NAME;
            }

            OM_uint32 minor_status = 0;
            [[maybe_unused]] OM_uint32 major_status = gss_release_buffer(
                &minor_status,
                &output_token_buf
            );
        });

        OM_uint32 minor_status = 0;
        OM_uint32 major_status = gss_accept_sec_context(
            &minor_status,
            &context_handle,
            acceptor_credentials_handle,
            &input_token_buf,
            GSS_C_NO_CHANNEL_BINDINGS,
            &initiator_name,
            &mech_type,
            &output_token_buf,
            &flags,
            nullptr,
            nullptr
        );

        if (major_status == GSS_S_COMPLETE)
        {
            if (!params.mechanism.empty() && !equalMechanisms(params.mechanism, mech_type))
                throw Exception("gss_accept_sec_context() succeeded, but: the authentication mechanism is not what was expected", ErrorCodes::KERBEROS_ERROR);

            if (flags & GSS_C_ANON_FLAG)
                throw Exception("gss_accept_sec_context() succeeded, but: the initiator does not wish to be authenticated", ErrorCodes::KERBEROS_ERROR);

            std::tie(user_name, realm) = extractNameAndRealm(initiator_name);

            if (user_name.empty())
                throw Exception("gss_accept_sec_context() succeeded, but: the initiator name cannot be extracted", ErrorCodes::KERBEROS_ERROR);

            if (realm.empty())
                throw Exception("gss_accept_sec_context() succeeded, but: the initiator realm cannot be extracted", ErrorCodes::KERBEROS_ERROR);

            if (!params.realm.empty() && params.realm != realm)
                throw Exception("gss_accept_sec_context() succeeded, but: the initiator realm is not what was expected (expected: " + params.realm + ", actual: " + realm + ")", ErrorCodes::KERBEROS_ERROR);

            output_token = bufferToString(output_token_buf);

            is_ready = true;
            is_failed = false;

            resetHandles();
        }
        else if (!GSS_ERROR(major_status) && (major_status & GSS_S_CONTINUE_NEEDED))
        {
            output_token = bufferToString(output_token_buf);

            is_ready = false;
            is_failed = false;
        }
        else
        {
            const auto messages = extractStatusMessages(major_status, minor_status, mech_type);
            throw Exception("gss_accept_sec_context() failed" + (messages.empty() ? "" : ": " + messages), ErrorCodes::KERBEROS_ERROR);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Could not process GSS token");

        is_ready = true;
        is_failed = true;

        resetHandles();
    }

    return output_token;
}

#else // USE_KRB5

void GSSAcceptorContext::reset()
{
}

void GSSAcceptorContext::resetHandles() noexcept
{
}

void GSSAcceptorContext::initHandles()
{
}

String GSSAcceptorContext::processToken(const String &, Poco::Logger *)
{
    throw Exception("ClickHouse was built without GSS-API/Kerberos support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
}

#endif // USE_KRB5

}
