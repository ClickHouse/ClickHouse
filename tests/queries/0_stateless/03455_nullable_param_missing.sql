SET enable_analyzer = 1;

-- Omitted parameter `p` declared as Nullable(Int64) should be treated as NULL
SELECT {p:Nullable(Int64)} IS NULL;
