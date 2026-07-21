#pragma once

#include <config.h>

#if USE_JWT_CPP && USE_SSL

#include <condition_variable>
#include <mutex>
#include <string>

namespace Poco::Net
{
class HTTPRequestHandlerFactory;
}

namespace DB
{

/// Shared state between the loopback OAuth authorization-code callback server
/// and the flow that waits on it. Populated by the first `/callback` delivery
/// whose `state` matches `expected_state`.
///
/// This type and the factory below are production internals of
/// `runOAuthAuthCodeFlow`; they live in their own header (rather than in the
/// widely-included `OAuthFlowRunner.h`) only so the regression tests can drive
/// the real callback handler over loopback HTTP without widening the surface
/// every other OAuth translation unit sees.
struct OAuthCallbackState
{
    std::mutex mtx;
    std::condition_variable cv;

    /// The CSRF state the flow expects the IdP to echo back. Set before the
    /// server starts; read-only afterwards. The handler unblocks the flow only
    /// for a `/callback` carrying exactly this value, so a local process that
    /// does not know the state cannot pre-empt the flow with a forged callback.
    std::string expected_state;

    std::string code;
    std::string error;
    bool done = false;

    /// Set (but never unblocks the flow) when a `/callback` arrived whose
    /// `state` did not match `expected_state`. Lets the waiting flow, if it
    /// times out, report a likely CSRF/state-mismatch instead of a bare
    /// timeout — recovering the diagnostic that the removed main-thread check
    /// used to give — without letting the mismatched callback abort the flow.
    bool saw_state_mismatch = false;
};

/// Build the production request-handler factory for the loopback callback
/// server. The returned pointer is owned by the caller (Poco::Net::HTTPServer
/// takes ownership when constructed with it). The server it builds recognizes
/// only `/callback`; every other path returns 404 and never serves the
/// authorization URL or CSRF state back to a caller.
Poco::Net::HTTPRequestHandlerFactory * createOAuthCallbackHandlerFactory(OAuthCallbackState & state);

}

#endif // USE_JWT_CPP && USE_SSL
