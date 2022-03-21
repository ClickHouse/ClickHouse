select getServerPort('tcp_port');

select getServerPort('unknown'); -- { serverError 170 }
