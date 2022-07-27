set dialect='kusto';
print '-- ipv4_is_private(\'127.0.0.1\')';
print ipv4_is_private('127.0.0.1');
print '-- ipv4_is_private(\'10.1.2.3\')';
print ipv4_is_private('10.1.2.3');
print '-- ipv4_is_private(\'192.168.1.1/24\')';
print ipv4_is_private('192.168.1.1/24');
print 'ipv4_is_private(strcat(\'192.\',\'168.\',\'1.\',\'1\',\'/24\'))';
print ipv4_is_private(strcat('192.','168.','1.','1','/24'));
print '-- ipv4_is_private(\'abc\')';
print ipv4_is_private('abc'); -- == null

print '-- ipv4_netmask_suffix(\'192.168.1.1/24\')';
print ipv4_netmask_suffix('192.168.1.1/24'); -- == 24
print '-- ipv4_netmask_suffix(\'192.168.1.1\')';
print ipv4_netmask_suffix('192.168.1.1'); -- == 32
print '-- ipv4_netmask_suffix(\'127.0.0.1/16\')';
print ipv4_netmask_suffix('127.0.0.1/16'); -- == 16
print '-- ipv4_netmask_suffix(\'abc\')';
print ipv4_netmask_suffix('abc'); -- == null
print 'ipv4_netmask_suffix(strcat(\'127.\', \'0.\', \'0.1/16\'))';
print ipv4_netmask_suffix(strcat('127.', '0.', '0.1/16')); -- == 16

print '-- ipv4_is_in_range(\'127.0.0.1\', \'127.0.0.1\')';
print ipv4_is_in_range('127.0.0.1', '127.0.0.1'); -- == true
print '-- ipv4_is_in_range(\'192.168.1.6\', \'192.168.1.1/24\')';
print ipv4_is_in_range('192.168.1.6', '192.168.1.1/24'); -- == true
print '-- ipv4_is_in_range(\'192.168.1.1\', \'192.168.2.1/24\')';
print ipv4_is_in_range('192.168.1.1', '192.168.2.1/24'); -- == false
print 'ipv4_is_in_range(strcat(\'192.\',\'168.\', \'1.1\'), \'192.168.2.1/24\')';
print ipv4_is_in_range(strcat('192.','168.', '1.1'), '192.168.2.1/24'); -- == false

-- TODO:
-- print ipv4_is_in_range('abc', '127.0.0.1'); -- == null
-- parse_ipv4()
-- parse_ipv6()