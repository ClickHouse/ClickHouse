//#pragma once
//
//#include <IO/ReadBufferFromPocoSocket.h>
//#include <IO/WriteBufferFromPocoSocket.h>
//
//namespace DB
//{
//
///// TODO if any fragment queryPipline exception send Exception
///// TODO for loop (interactive_delay / 1000)
///// TODO receive query finish or cancel
///// TODO sendProfileInfo(executor.getProfileInfo());
////       sendProgress();
////       sendLogs();
////       sendSelectProfileEvents();
//
//class QueryStatusReporter
//{
//public:
//    void reporter()
//    {
//        Block block;
//        while (executor.pull(block, interactive_delay / 1000))
//        {
//            std::unique_lock lock(task_callback_mutex);
//
//            auto cancellation_status = getQueryCancellationStatus();
//            if (cancellation_status == CancellationStatus::FULLY_CANCELLED)
//            {
//                /// Several callback like callback for parallel reading could be called from inside the pipeline
//                /// and we have to unlock the mutex from our side to prevent deadlock.
//                lock.unlock();
//                /// A packet was received requesting to stop execution of the request.
//                executor.cancel();
//                break;
//            }
//            else if (cancellation_status == CancellationStatus::READ_CANCELLED)
//            {
//                executor.cancelReading();
//            }
//
//            /// TODO like RemoteQueryExecutor ？
//            /// TODO if cancellation_status and query coordination, send cancel to other node
//            /// TODO if on query coordination, receive exception from other node connections, if any one exception, rethrow, send cancel to other node
//            /// TODO receiveProfileInfo(executor.getProfileInfo());
//            //       receiveProgress(); updateProgress
//            //       receiveLogs();
//            //       receiveSelectProfileEvents();
//
//            if (after_send_progress.elapsed() / 1000 >= interactive_delay)
//            {
//                /// Some time passed and there is a progress.
//                after_send_progress.restart();
//                sendProgress();
//                sendSelectProfileEvents();
//            }
//
//            sendLogs();
//
//            if (block)
//            {
//                if (!state.io.null_format)
//                    sendData(block);
//            }
//        }
//    }
//
//private:
//    /// Streams for reading/writing from/to client connection socket.
//    std::shared_ptr<ReadBuffer> in;
//    std::shared_ptr<WriteBuffer> out;
//
//
//};
//
//}
