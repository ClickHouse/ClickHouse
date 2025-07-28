WITH 0 AS n SELECT multiIf(n = 0, 42, intDiv(100, n));
WITH 0 AS n SELECT multiIf(n = 1, 42, intDiv(100, n)); -- { serverError ILLEGAL_DIVISION }

WITH 0 AS n SELECT multiIf(n = 0, intDiv(100, n), 42); -- { serverError ILLEGAL_DIVISION }
WITH 0 AS n SELECT multiIf(n = 1, intDiv(100, n), 42);

WITH 0 AS n SELECT multiIf(n = 0, 42, n = 1, 24, intDiv(100, n));
WITH 0 AS n SELECT multiIf(n = 1, 42, n = 0, 24, intDiv(100, n));
WITH 0 AS n SELECT multiIf(n = 0, 42, n = 1, intDiv(100, n), 24);
WITH 0 AS n SELECT multiIf(n = 1, 42, n = 0, intDiv(100, n), 24); -- { serverError ILLEGAL_DIVISION }

WITH 0 AS n SELECT multiIf(n = 0, intDiv(100, n), n = 1, 42, intDiv(100, n)); -- { serverError ILLEGAL_DIVISION }
WITH 0 AS n SELECT multiIf(n = 1, intDiv(100, n), n = 0, 42, intDiv(100, n));
WITH 0 AS n SELECT multiIf(n = 0, intDiv(100, n), n = 1, intDiv(100, n), 42); -- { serverError ILLEGAL_DIVISION }
WITH 0 AS n SELECT multiIf(n = 1, intDiv(100, n), n = 0, intDiv(100, n), 42); -- { serverError ILLEGAL_DIVISION }
