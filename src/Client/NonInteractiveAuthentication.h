#pragma once

namespace DB
{

/// A background job authenticates on a thread of its own, while the main thread owns the terminal and its line
/// editor. Whatever on that thread would ask the user something - the passphrase of an SSH key file, the device
/// code login of an OAuth identity provider - would compete with the main thread for the terminal, and could not be
/// cancelled while it waits. While this object is alive on a thread, such code must not ask, and must fail with an
/// explicit error instead. The main thread asks beforehand (`ConnectionParameters::renewSecondFactor`) or when
/// the user runs the next query.
class NonInteractiveAuthenticationScope
{
public:
    NonInteractiveAuthenticationScope();
    ~NonInteractiveAuthenticationScope();

    NonInteractiveAuthenticationScope(const NonInteractiveAuthenticationScope &) = delete;
    NonInteractiveAuthenticationScope & operator=(const NonInteractiveAuthenticationScope &) = delete;

    /// True on a thread that must not ask the user anything.
    static bool isActive();

private:
    bool previous;
};

}
