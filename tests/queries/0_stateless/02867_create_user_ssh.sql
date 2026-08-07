-- Tags: no-fasttest, no-parallel

-- Tests user authentication with SSH public keys

DROP USER IF EXISTS test_user_02867;

-- negative tests
CREATE USER test_user_02867 IDENTIFIED WITH ssh_key BY KEY 'invalid_key' TYPE 'ssh-rsa'; -- { serverError LIBSSH_ERROR }
CREATE USER test_user_02867 IDENTIFIED WITH ssh_key BY KEY 'invalid_key' TYPE 'ssh-rsa', KEY 'invalid_key' TYPE 'ssh-rsa'; -- { serverError LIBSSH_ERROR }
CREATE USER test_user_02867 IDENTIFIED WITH ssh_key
BY KEY 'AAAAB3NzaC1yc2EAAAADAQABAAABgQCVTUso7/LQcBljfsHwyuL6fWfIvS3BaVpYB8lwf/ZylSOltBy6YlABtTU3mIb197d2DW99RcLKk174f5Zj5rUukXbV0fnufWvwd37fbb1eKM8zxBYvXs53EI5QBPZgKACIzMpYYZeJnAP0oZhUfWWtKXpy/SQ5CHiEIGD9RNYDL+uXZejMwC5r/+f2AmrATBo+Y+WJFZIvhj4uznFYvyvNTUz/YDvZCk+vwwIgiv4BpFCaZm2TeETTj6SvK567bZznLP5HXrkVbB5lhxjAkahc2w/Yjm//Fwto3xsMoJwROxJEU8L1kZ40QWPqjo7Tmr6C/hL2cKDNgWOEqrjLKQmh576s1+PfxwXpVPjLK4PHVSvuJLV88sn0iPdspLlKlDCdc7T9MqIrjJfxuhqnaoFQ7U+oBte8vkm1wGu76+WEC3iNWVAiIVZxLx9rUEsDqj3OovqfLiRsTmNLeY94p2asZjkx7rU48ZwuYN5XGafYsArPscj9Ve6RoRrof+5Q7cc='
TYPE 'invalid_algorithm'; -- { serverError LIBSSH_ERROR }

CREATE USER test_user_02867 IDENTIFIED WITH ssh_key
BY KEY 'AAAAB3NzaC1yc2EAAAADAQABAAABgQCVTUso7/LQcBljfsHwyuL6fWfIvS3BaVpYB8lwf/ZylSOltBy6YlABtTU3mIb197d2DW99RcLKk174f5Zj5rUukXbV0fnufWvwd37fbb1eKM8zxBYvXs53EI5QBPZgKACIzMpYYZeJnAP0oZhUfWWtKXpy/SQ5CHiEIGD9RNYDL+uXZejMwC5r/+f2AmrATBo+Y+WJFZIvhj4uznFYvyvNTUz/YDvZCk+vwwIgiv4BpFCaZm2TeETTj6SvK567bZznLP5HXrkVbB5lhxjAkahc2w/Yjm//Fwto3xsMoJwROxJEU8L1kZ40QWPqjo7Tmr6C/hL2cKDNgWOEqrjLKQmh576s1+PfxwXpVPjLK4PHVSvuJLV88sn0iPdspLlKlDCdc7T9MqIrjJfxuhqnaoFQ7U+oBte8vkm1wGu76+WEC3iNWVAiIVZxLx9rUEsDqj3OovqfLiRsTmNLeY94p2asZjkx7rU48ZwuYN5XGafYsArPscj9Ve6RoRrof+5Q7cc='
TYPE 'ssh-rsa';

SHOW CREATE USER test_user_02867;

DROP USER test_user_02867;
