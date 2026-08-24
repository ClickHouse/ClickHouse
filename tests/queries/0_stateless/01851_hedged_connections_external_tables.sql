-- Tags: no-tsan
select number from remote('127.0.0.{3|2}', numbers(2)) where number global in (select number from numbers(1))　settings async_socket_for_remote=1, use_hedged_requests = 1, sleep_in_send_data_ms=10, receive_data_timeout_ms=1;
