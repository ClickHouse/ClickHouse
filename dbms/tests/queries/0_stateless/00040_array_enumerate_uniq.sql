SELECT max(arrayJoin(arr)) FROM (SELECT arrayEnumerateUniq(groupArray(intDiv(number, 54321)) AS nums, groupArray(toString(intDiv(number, 98765)))) AS arr FROM (SELECT number FROM system.numbers LIMIT 1000000) GROUP BY intHash32(number) % 100000)

