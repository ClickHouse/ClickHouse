#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Server/WebSocket/WebSocket.h>
#include <IO/Progress.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBuffer.h>


namespace DB {

    class WriteBufferFromWebSocket : public BufferWithOwnMemory<WriteBuffer>
{
public:
    WriteBufferFromWebSocket(WebSocket & ws_, bool send_progress_ = false);

    ~WriteBufferFromWebSocket() override;

    void onProgress(const Progress & progress);

    void closeWithException(int exception_bitcode, std::string exception_text);

    void setQueryId(std::string query_id_) {query_id = query_id_;}

private:


    void nextImpl() override;

    void finalizeImpl() override;

    void SendDataMessage(bool is_last_message = false);

    void SendProgressMessage();

    void ConstructDataMessage(WriteBuffer & message, bool is_last_message = false);

    void ConstructProgressMessage(WriteBuffer & message);

    void SendMessage(std::string & message);

    int max_payload_size = 100000;
    std::string query_id = "";
    Progress accumulated_progress;
    std::mutex mutex;

    std::stringstream data_stream;
    std::unique_ptr<WriteBufferFromOStream> out;


    Stopwatch progress_watch;
    bool send_progress = false;
    size_t send_progress_interval_ms = 100;

    WebSocket & ws;

};

}
