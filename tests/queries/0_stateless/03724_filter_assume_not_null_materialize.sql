SET optimize_move_to_prewhere = 1;

SELECT (assumeNotNull((NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT (assumeNotNull(materialize(NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(NULL), 1) = (1, 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(materialize(NULL)), 1) = (1, 1); -- { serverError ILLEGAL_COLUMN }

SELECT (assumeNotNull((NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT (assumeNotNull(materialize(NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(NULL), 1) = (1, 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(materialize(NULL)), 1) = (1, 1); -- { serverError ILLEGAL_COLUMN }
