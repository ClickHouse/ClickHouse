SET send_logs_level = 'none';

SET max_block_size = 0;
SELECT number FROM system.numbers; -- { serverError 12 }
