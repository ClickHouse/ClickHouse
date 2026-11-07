#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>

namespace DB
{

/// Forwards everything written to it to the underlying buffer with the bytes a terminal would
/// interpret as controls made visible, so that the output cannot take control of the terminal
/// (clear the screen, move the cursor, spoof the prompt, write to the clipboard).
///
/// The C0 controls except the line feed and the tab, `ESC` among them, and DEL are replaced with
/// their Unicode "Control Pictures" (U+2400..U+2421). The C1 controls U+0080..U+009F (UTF-8
/// `C2 80`..`C2 9F`), which open CSI/OSC sequences without any `ESC` on the terminals that honor
/// them, and stray bytes that are not a part of a valid UTF-8 sequence, which an 8-bit terminal
/// could read as C1 controls, are replaced with U+FFFD. This is the streaming counterpart of
/// `AIAgentDisplay::sanitizeForTerminal`: a multi-byte sequence split between two chunks is held
/// back until it is complete.
///
/// It is used for the output of the queries the AI agent runs without asking the user: their
/// result is chosen by the model, which can be prompt-injected through the data it reads.
class WriteBufferSanitizingTerminalOutput final : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit WriteBufferSanitizingTerminalOutput(WriteBuffer & out_);
    ~WriteBufferSanitizingTerminalOutput() override;

private:
    void nextImpl() override;
    void finalizeImpl() override;

    void process(const char * begin, const char * end);

    WriteBuffer & out;

    /// The beginning of a multi-byte sequence that the last chunk ended in the middle of.
    char pending[4]{};
    size_t pending_size = 0;
};

}
