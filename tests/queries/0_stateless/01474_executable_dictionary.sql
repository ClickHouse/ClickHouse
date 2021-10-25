-- Tags: no-tsan, no-parallel
-- Tag no-tsan: informational stderr from sanitizer at start

SELECT number, dictGet('executable_complex', 'a', (number, number)) AS a, dictGet('executable_complex', 'b', (number, number)) AS b FROM numbers(1000000) WHERE number = 999999;
SELECT number, dictGet('executable_complex_direct', 'a', (number, number)) AS a, dictGet('executable_complex_direct', 'b', (number, number)) AS b FROM numbers(1000000) WHERE number = 999999;
SELECT number, dictGet('executable_simple', 'a', number) AS a, dictGet('executable_simple', 'b', number) AS b FROM numbers(1000000) WHERE number = 999999;
