---
title: Solução de problemas
---

[//]: # "Este arquivo está incluído em FAQ > Solução de problemas"

* [Instalação](#troubleshooting-installation-errors)
* [Conexão com o servidor](#troubleshooting-accepts-no-connections)
* [Processamento de consultas](#troubleshooting-does-not-process-queries)
* [Eficiência do processamento de consultas](#troubleshooting-too-slow)

<div id="troubleshooting-installation-errors">
  ## Instalação
</div>

<div id="you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get">
  ### Não é possível obter pacotes deb do repositório do ClickHouse com o apt-get
</div>

* Verifique as configurações do firewall.
* Se não for possível acessar o repositório por qualquer motivo, baixe os pacotes conforme descrito no artigo [guia de instalação](../getting-started/install.md) e instale-os manualmente com o comando `sudo dpkg -i <packages>`. Você também precisará do pacote `tzdata`.

<div id="you-cannot-update-deb-packages-from-clickhouse-repository-with-apt-get">
  ### Não é possível atualizar pacotes deb do repositório do ClickHouse com apt-get
</div>

* Isso pode acontecer quando a chave GPG é alterada.

Use as instruções na página de [configuração](../getting-started/install.md#setup-the-debian-repository) para atualizar a configuração do repositório.

<div id="you-get-different-warnings-with-apt-get-update">
  ### Você recebe avisos diferentes ao executar `apt-get update`
</div>

* As mensagens de aviso exibidas podem ser uma das seguintes:

```bash
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository 'https://packages.clickhouse.com/deb stable InRelease' doesn't support architecture 'i386'
```

```bash
E: Failed to fetch https://packages.clickhouse.com/deb/dists/stable/main/binary-amd64/Packages.gz  File has unexpected size (30451 != 28154). Mirror sync in progress?
```

```text
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Origin' value from 'Artifactory' to 'ClickHouse'
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Label' value from 'Artifactory' to 'ClickHouse'
N: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Suite' value from 'stable' to ''
N: This must be accepted explicitly before updates for this repository can be applied. See apt-secure(8) manpage for details.
```

```bash
Err:11 https://packages.clickhouse.com/deb stable InRelease
  400  Bad Request [IP: 172.66.40.249 443]
```

Para resolver o problema acima, use o script a seguir:

```bash
sudo rm /var/lib/apt/lists/packages.clickhouse.com_* /var/lib/dpkg/arch /var/lib/apt/lists/partial/packages.clickhouse.com_*
sudo apt-get clean
sudo apt-get autoclean
```

<div id="you-cant-get-packages-with-yum-because-of-wrong-signature">
  ### Você não consegue instalar pacotes com o yum por causa de uma assinatura incorreta
</div>

Possível problema: o cache está incorreto; talvez ele tenha sido corrompido após a atualização da chave GPG em 2022-09.

A solução é limpar o cache e o diretório lib do yum:

```bash
sudo find /var/lib/yum/repos/ /var/cache/yum/ -name 'clickhouse-*' -type d -exec rm -rf {} +
sudo rm -f /etc/yum.repos.d/clickhouse.repo
```

Depois, siga o [guia de instalação](../getting-started/install.md#from-rpm-packages)

<div id="you-cant-run-docker-container">
  ### Você não consegue iniciar o contêiner Docker
</div>

Ao executar um simples `docker run clickhouse/clickhouse-server`, ele falha com um stack trace semelhante ao seguinte:

```bash
$ docker run -it clickhouse/clickhouse-server
........
Poco::Exception. Code: 1000, e.code() = 0, System exception: cannot start thread, Stack trace (when copying this message, always include the lines below):

0. Poco::ThreadImpl::startImpl(Poco::SharedPtr<Poco::Runnable, Poco::ReferenceCounter, Poco::ReleasePolicy<Poco::Runnable>>) @ 0x00000000157c7b34
1. Poco::Thread::start(Poco::Runnable&) @ 0x00000000157c8a0e
2. BaseDaemon::initializeTerminationAndSignalProcessing() @ 0x000000000d267a14
3. BaseDaemon::initialize(Poco::Util::Application&) @ 0x000000000d2652cb
4. DB::Server::initialize(Poco::Util::Application&) @ 0x000000000d128b38
5. Poco::Util::Application::run() @ 0x000000001581cfda
6. DB::Server::run() @ 0x000000000d1288f0
7. Poco::Util::ServerApplication::run(int, char**) @ 0x0000000015825e27
8. mainEntryClickHouseServer(int, char**) @ 0x000000000d125b38
9. main @ 0x0000000007ea4eee
10. ? @ 0x00007f67ff946d90
11. ? @ 0x00007f67ff946e40
12. _start @ 0x00000000062e802e
 (version 24.10.1.2812 (official build))
```

O motivo é um daemon do Docker antigo com versão inferior à `20.10.10`. Uma forma de corrigir isso é atualizá-lo ou executar `docker run [--privileged | --security-opt seccomp=unconfined]`. A segunda opção tem implicações de segurança.

<div id="troubleshooting-accepts-no-connections">
  ## Conectando-se ao servidor
</div>

Possíveis problemas:

* O servidor não está em execução.
* Parâmetros de configuração inesperados ou incorretos.

<div id="server-is-not-running">
  ### O servidor não está em execução
</div>

**Verifique se o servidor está em execução**

Comando:

```bash
$ sudo service clickhouse-server status
```

Se o servidor não estiver em execução, inicie-o com o comando:

```bash
$ sudo service clickhouse-server start
```

**Verifique os logs**

Por padrão, o log principal do `clickhouse-server` fica em `/var/log/clickhouse-server/clickhouse-server.log`.

Se o servidor tiver sido iniciado com sucesso, você deverá ver as seguintes strings:

* `<Information> Application: starting up.` — Servidor iniciado.
* `<Information> Application: Ready for connections.` — O servidor está em execução e pronto para aceitar conexões.

Se a inicialização do `clickhouse-server` falhar devido a um erro de configuração, você deverá ver a string `<Error>` com a descrição do erro. Por exemplo:

```text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

Se você não vir um erro no final do arquivo, examine o arquivo inteiro a partir da string:

```text
<Information> Application: starting up.
```

Se você tentar iniciar uma segunda instância de `clickhouse-server` no servidor, será exibido o seguinte log:

```text
2019.01.11 15:25:11.151730 [ 1 ] {} <Information> : Starting ClickHouse 19.1.0 with revision 54413
2019.01.11 15:25:11.154578 [ 1 ] {} <Information> Application: starting up
2019.01.11 15:25:11.156361 [ 1 ] {} <Information> StatusFile: Status file ./status already exists - unclean restart. Contents:
PID: 8510
Started at: 2019-01-11 15:24:23
Revision: 54413

2019.01.11 15:25:11.156673 [ 1 ] {} <Error> Application: DB::Exception: Cannot lock file ./status. Another server instance in same directory is already running.
2019.01.11 15:25:11.156682 [ 1 ] {} <Information> Application: shutting down
2019.01.11 15:25:11.156686 [ 1 ] {} <Debug> Application: Uninitializing subsystem: Logging Subsystem
2019.01.11 15:25:11.156716 [ 2 ] {} <Information> BaseDaemon: Stop SignalListener thread
```

**Ver os logs do system.d**

Se você não encontrar nenhuma informação útil nos logs do `clickhouse-server` ou se não houver logs, poderá ver os logs de `system.d` usando o comando:

```bash
$ sudo journalctl -u clickhouse-server
```

**Inicie o clickhouse-server em modo interativo**

```bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

Este comando inicia o servidor como uma aplicação interativa, com os parâmetros padrão do script de inicialização automática. Nesse modo, o `clickhouse-server` imprime todas as mensagens de evento no console.

<div id="configuration-parameters">
  ### Parâmetros de configuração
</div>

Verifique:

* Configurações do Docker.

  Se você executar o ClickHouse no Docker em uma rede IPv6, certifique-se de que `network=host` esteja definido.

* Configurações do endpoint.

  Verifique as configurações [listen&#95;host](../operations/server-configuration-parameters/settings.md#listen_host) e [tcp&#95;port](../operations/server-configuration-parameters/settings.md#tcp_port).

  Por padrão, o servidor ClickHouse aceita apenas conexões de localhost.

* Configurações do protocolo HTTP.

  Verifique as configurações de protocolo da API HTTP.

* Configurações de conexão segura.

  Verifique:

  * A configuração [tcp&#95;port&#95;secure](../operations/server-configuration-parameters/settings.md#tcp_port_secure).
  * As configurações de [certificados SSL](../operations/server-configuration-parameters/settings.md#openssl).

    Use os parâmetros adequados ao se conectar. Por exemplo, use o parâmetro `port_secure` com `clickhouse_client`.

* Configurações do usuário.

  Você pode estar usando o nome de usuário ou a senha incorretos.

<div id="troubleshooting-does-not-process-queries">
  ## Processamento de consultas
</div>

Se o ClickHouse não conseguir processar a consulta, ele envia uma descrição do erro ao cliente. No `clickhouse-client`, a descrição do erro é exibida no terminal. Se você estiver usando a interface HTTP, o ClickHouse envia a descrição do erro no corpo da resposta. Por exemplo:

```bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

Se você iniciar o `clickhouse-client` com o parâmetro `stack-trace`, o ClickHouse retornará a stack trace do servidor com a descrição de um erro.

Você poderá ver uma mensagem sobre uma conexão interrompida. Nesse caso, pode repetir a consulta. Se a conexão for interrompida sempre que você executar a consulta, verifique os logs do servidor em busca de erros.

<div id="troubleshooting-too-slow">
  ## Eficiência do processamento de consultas
</div>

Se você perceber que o ClickHouse está funcionando muito lentamente, será necessário avaliar a carga sobre os recursos do servidor e a rede em relação às suas consultas.

Você pode usar o utilitário `clickhouse-benchmark` para analisar o desempenho das consultas. Ele mostra o número de consultas processadas por segundo, o número de linhas processadas por segundo e os percentis dos tempos de processamento das consultas.