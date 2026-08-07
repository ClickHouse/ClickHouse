DROP TABLE IF EXISTS table_with_cyclic_defaults;

CREATE TABLE table_with_cyclic_defaults (a DEFAULT b, b DEFAULT a) ENGINE = Memory; --{serverError CYCLIC_ALIASES}

CREATE TABLE table_with_cyclic_defaults (a DEFAULT b + 1, b DEFAULT a * a) ENGINE = Memory; --{serverError CYCLIC_ALIASES}

CREATE TABLE table_with_cyclic_defaults (a DEFAULT b, b DEFAULT toString(c), c DEFAULT concat(a, '1')) ENGINE = Memory; --{serverError CYCLIC_ALIASES}

CREATE TABLE table_with_cyclic_defaults (a DEFAULT b, b DEFAULT c, c DEFAULT a * b) ENGINE = Memory; --{serverError CYCLIC_ALIASES}

CREATE TABLE table_with_cyclic_defaults (a String DEFAULT b, b String DEFAULT a) ENGINE = Memory; --{serverError CYCLIC_ALIASES}

CREATE TABLE table_with_cyclic_defaults (a String) ENGINE = Memory;

ALTER TABLE table_with_cyclic_defaults ADD COLUMN c String DEFAULT b, ADD COLUMN b String DEFAULT c; --{serverError CYCLIC_ALIASES}

ALTER TABLE table_with_cyclic_defaults ADD COLUMN b String DEFAULT a, MODIFY COLUMN a DEFAULT b; --{serverError CYCLIC_ALIASES}

SELECT 1;

DROP TABLE IF EXISTS table_with_cyclic_defaults;
