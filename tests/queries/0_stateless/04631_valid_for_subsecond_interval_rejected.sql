-- The VALID FOR deadline is sampled, stored and enforced with second precision, so a sub-second
-- interval could not take effect as requested (e.g. VALID FOR INTERVAL 1 MILLISECOND would keep the
-- credential usable until the next whole second). Sub-second interval kinds are rejected instead of
-- silently truncated. Every statement here fails, so no user is ever created and no cleanup is needed.

CREATE USER user_04631_subsecond VALID FOR INTERVAL 1 MILLISECOND; -- { serverError BAD_ARGUMENTS }
CREATE USER user_04631_subsecond VALID FOR INTERVAL 1 MICROSECOND; -- { serverError BAD_ARGUMENTS }
CREATE USER user_04631_subsecond VALID FOR INTERVAL 1 NANOSECOND; -- { serverError BAD_ARGUMENTS }

-- A sub-second term hiding inside a sum of intervals is rejected too.
CREATE USER user_04631_subsecond VALID FOR INTERVAL 1 DAY + INTERVAL 1 MILLISECOND; -- { serverError BAD_ARGUMENTS }

-- Sub-second intervals are also rejected at the credential level.
CREATE USER user_04631_subsecond IDENTIFIED WITH plaintext_password BY 'x' VALID FOR INTERVAL 500 MILLISECOND; -- { serverError BAD_ARGUMENTS }

-- A multiple of a sub-second unit that is a whole number of seconds is still rejected: the unit
-- itself is sub-second, and accepting 1000 but not 999 would be a confusing contract.
CREATE USER user_04631_subsecond VALID FOR INTERVAL 1000 MILLISECOND; -- { serverError BAD_ARGUMENTS }
