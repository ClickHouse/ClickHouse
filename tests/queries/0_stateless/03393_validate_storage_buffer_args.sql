CREATE TABLE invalid (c0 Int) ENGINE = Buffer(x, x, 0, 1, 1, 1, 1, 1, 1); -- {serverError BAD_ARGUMENTS} must be a positive integer
CREATE TABLE invalid (c0 Int) ENGINE = Buffer(x, x, -1, 1, 1e6, 1, 1, 1, 1); -- {serverError BAD_ARGUMENTS} must be non-negative value
CREATE TABLE invalid (c0 Int) ENGINE = Buffer(x, x, 1, 1, -1, 1, 1, 1, 1); -- {serverError BAD_ARGUMENTS} must be non-negative value
CREATE TABLE invalid (c0 Int) ENGINE = Buffer(x, x, 1, 1, 1, 1, 1, -1e6, 1); -- {serverError BAD_ARGUMENTS} must be non-negative value
