-- Hashing an AggregateFunction column serializes every state. If any serialized byte fails to reach
-- the hash, every state hashes alike and counting distinct states silently collapses to one.
-- The second query's states outgrow the window those bytes are hashed over, and differ only early.
SELECT uniqExact(st), uniq(st)
FROM (SELECT number AS k, groupArrayState(toString(number)) AS st FROM numbers(50) GROUP BY k);

SELECT uniqExact(st)
FROM (SELECT number AS k, groupArrayState(concat(toString(number), repeat('x', 2000))) AS st
      FROM numbers(50) GROUP BY k);
