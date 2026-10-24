-- Tags: no-fasttest
-- Tag no-fasttest: the RabbitMQ engine is not built in the fast test.

-- Malformed routing keys are rejected before connecting to the broker,
-- so the host below does not need to exist.

-- A routing key without `=` for the `headers` exchange used to cause an out-of-bounds read.
CREATE TABLE t_rmq_headers (x String) ENGINE = RabbitMQ
SETTINGS rabbitmq_host_port = 'localhost:5672', rabbitmq_username = 'u', rabbitmq_password = 'p',
    rabbitmq_exchange_name = 'e', rabbitmq_exchange_type = 'headers',
    rabbitmq_routing_key_list = 'format,logs', rabbitmq_format = 'JSONEachRow'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_rmq_headers (x String) ENGINE = RabbitMQ
SETTINGS rabbitmq_host_port = 'localhost:5672', rabbitmq_username = 'u', rabbitmq_password = 'p',
    rabbitmq_exchange_name = 'e', rabbitmq_exchange_type = 'headers',
    rabbitmq_routing_key_list = 'x-match=all,format', rabbitmq_format = 'JSONEachRow'; -- { serverError BAD_ARGUMENTS }
