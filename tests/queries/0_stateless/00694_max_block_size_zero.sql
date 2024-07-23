SET send_logs_level = 'fatal';

SET max_block_size = 0;
SELECT number FROM system.numbers; -- { serverError 12 }
