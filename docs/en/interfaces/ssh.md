---
description: 'Documentation for the SSH interface in ClickHouse'
keywords: ['client', 'ssh', 'putty']
sidebar_label: 'SSH Interface'
sidebar_position: 60
slug: /interfaces/ssh
title: 'SSH Interface'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# SSH interface with PTY

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

## Preface {#preface}

ClickHouse server allows to connect to itself directly using the SSH protocol. Any client is allowed.


After creating a [database user identified by an SSH key](/knowledgebase/how-to-connect-to-ch-cloud-using-ssh-keys):
```sql
CREATE USER abcuser IDENTIFIED WITH ssh_key BY KEY '<REDACTED>' TYPE 'ssh-ed25519';
```

You are able to use this key to connect to a ClickHouse server. It will open a pseudoterminal (PTY) with an interactive session of clickhouse-client.

```bash
> ssh -i ~/test_ssh/id_ed25519 abcuser@localhost -p 9022
ClickHouse embedded version 25.1.1.1.

ip-10-1-13-116.us-west-2.compute.internal :) SELECT 1;

SELECT 1

Query id: cdd91b7f-215b-4537-b7df-86d19bf63f64

   ┌─1─┐
1. │ 1 │
   └───┘

1 row in set. Elapsed: 0.002 sec.
```

The command execution over SSH (the non-interactive mode) is also supported:

```bash
> ssh -i ~/test_ssh/id_ed25519 abcuser@localhost -p 9022 "select 1"
1
```


## Server configuration {#server-configuration}

In order to enable the SSH server capability, you need to uncomment or place the following section in your `config.xml`:

```xml
<tcp_ssh_port>9022</tcp_ssh_port>
<ssh_server>
   <host_rsa_key>path-to-the-key</host_rsa_key>
   <!--host_ecdsa_key>path-to-the-key</host_ecdsa_key-->
   <!--host_ed25519_key>path-to-the-key</host_ed25519_key-->
</ssh_server>
```

The host key is an integral part of a SSH protocol. The public part of this key is stored in the `~/.ssh/known_hosts` file on the client side and typically needed to prevent man-in-the-middle type of attacks. When connecting to the server for the first time you will see the message below:

```shell
The authenticity of host '[localhost]:9022 ([127.0.0.1]:9022)' can't be established.
RSA key fingerprint is SHA256:3qxVlJKMr/PEKw/hfeg06HAK451Tt0eenhwqQvh58Do.
This key is not known by any other names
Are you sure you want to continue connecting (yes/no/[fingerprint])?
```

This, in fact means: "Do you want to remember the public key of this host and continue connecting?".

You can tell your SSH client not to verify the host by passing an option:

```bash
ssh -o "StrictHostKeyChecking no" user@host
```

## Configuring embedded client {#configuring-embedded-client}

You are able to pass options to an embedded client similar to the ordinary `clickhouse-client`, but with a few limitations.
Since this is an SSH protocol, the only way to pass parameters to the target host is through environment variables.

For example setting the `format` can be done this way:

```bash
> ssh -o SetEnv="format=Pretty" -i ~/test_ssh/id_ed25519  abcuser@localhost -p 9022 "SELECT 1"
   ┏━━━┓
   ┃ 1 ┃
   ┡━━━┩
1. │ 1 │
   └───┘
```

You are able to change any user-level setting this way and additionally pass most of the ordinary `clickhouse-client` options (except ones which don't make sense in this setup.)

Important:

In case if both `query` option and the SSH command is passed, the latter one is added to the list of queries to execute:

```bash
ubuntu ip-10-1-13-116@~$ ssh -o SetEnv="format=Pretty query=\"SELECT 2;\"" -i ~/test_ssh/id_ed25519  abcuser@localhost -p 9022 "SELECT 1"
   ┏━━━┓
   ┃ 2 ┃
   ┡━━━┩
1. │ 2 │
   └───┘
   ┏━━━┓
   ┃ 1 ┃
   ┡━━━┩
1. │ 1 │
   └───┘
```
