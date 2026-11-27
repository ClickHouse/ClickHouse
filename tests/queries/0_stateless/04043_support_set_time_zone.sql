SET TIME ZONE 'Абырвалг'; -- { serverError BAD_ARGUMENTS}

SET TIME ZONE 'UTC';
SELECT timezone();

SET TIME ZONE 'America/New_York';
SELECT timezone();

SET TIME ZONE 'Pacific/Pitcairn';
SELECT timezone(), timezoneOf(now());

SET TIME ZONE 'Asia/Novosibirsk';
SELECT timezone(), timezoneOf(now());

SET TIME ZONE = 'Абырвалг'; -- { serverError BAD_ARGUMENTS}

SET TIME ZONE = 'UTC';
SELECT timezone();

SET TIME ZONE = 'America/New_York';
SELECT timezone();

SET TIME ZONE = 'Pacific/Pitcairn';
SELECT timezone(), timezoneOf(now());

SET TIME ZONE = 'Asia/Novosibirsk';
SELECT timezone(), timezoneOf(now());

