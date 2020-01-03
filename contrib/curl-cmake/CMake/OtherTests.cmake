set(curl_cv_func_recv_args "int,void *,size_t,int,ssize_t")
set(RECV_TYPE_ARG1 "int")
set(RECV_TYPE_ARG2 "void *")
set(RECV_TYPE_ARG3 "size_t")
set(RECV_TYPE_ARG4 "int")
set(RECV_TYPE_RETV "ssize_t")
set(HAVE_RECV 1)
set(curl_cv_func_recv_done 1)

set(curl_cv_func_recv_args "${curl_cv_func_recv_args}" CACHE INTERNAL "Arguments for recv")
set(HAVE_RECV 1)


set(curl_cv_func_send_args "int,void *,size_t,int,ssize_t,const void *")
set(SEND_TYPE_ARG1 "int")
set(SEND_TYPE_ARG2 "void *")
set(SEND_TYPE_ARG3 "size_t")
set(SEND_TYPE_ARG4 "int")
set(SEND_TYPE_RETV "ssize_t")

set(curl_cv_func_send_args "${curl_cv_func_send_args}" CACHE INTERNAL "Arguments for send")
set(HAVE_SEND 1)

set (HAVE_MSG_NOSIGNAL 1)
set (HAVE_STRUCT_TIMEVAL 1)

set(HAVE_SIG_ATOMIC_T 1)
set(HAVE_STRUCT_SOCKADDR_STORAGE 1)
set(HAVE_POLL_FINE 1)
