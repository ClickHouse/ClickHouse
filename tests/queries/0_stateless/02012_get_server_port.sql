select getServerPort('tcp_port');

select getServerPort('unknown'); -- { serverError CLUSTER_DOESNT_EXIST }
