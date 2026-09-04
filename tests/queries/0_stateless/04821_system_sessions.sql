SELECT count() > 0 FROM system.sessions WHERE user = currentUser() AND interface = 'TCP';

SELECT count() FROM system.sessions WHERE auth_id = '00000000-0000-0000-0000-000000000000';
