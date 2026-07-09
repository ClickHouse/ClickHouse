---
description: 'Documentação do terminal web, uma sessão `clickhouse-client` no navegador via WebSocket'
sidebar_label: 'Terminal web'
sidebar_position: 22
slug: /interfaces/web-terminal
title: 'Terminal web'
doc_type: 'reference'
---

O terminal web é uma interface no navegador que fornece uma sessão interativa do `clickhouse-client` via WebSocket. Ele fica disponível em qualquer porta HTTP do ClickHouse no caminho `/webterminal`.

Acesse `/webterminal` em qualquer porta HTTP do ClickHouse (por exemplo, `http://localhost:8123/webterminal`) para abrir o terminal.

<div id="enabling-the-feature">
  ## Habilitando e desabilitando o recurso
</div>

O endpoint `/webterminal` vem habilitado por padrão e é controlado pela configuração de servidor `enable_webterminal`. Para desabilitá-lo, defina a configuração como `false`; as solicitações para `/webterminal` passarão a retornar o status HTTP `403 Forbidden`.

```xml
<clickhouse>
    <enable_webterminal>false</enable_webterminal>
</clickhouse>
```

:::note
`enable_webterminal` substitui a configuração anterior `allow_experimental_webterminal`. O nome antigo ainda é aceito por compatibilidade retroativa quando `enable_webterminal` não estiver definido.
:::

<div id="authentication">
  ## Autenticação
</div>

O terminal web autentica o usuário com as mesmas verificações de `Session` e de controle de acesso do protocolo HTTP, mas as credenciais são trocadas em banda pela conexão WebSocket já estabelecida, em vez de serem enviadas na solicitação de upgrade HTTP. Depois que o handshake do WebSocket é concluído, o navegador envia a primeira mensagem como JSON:

```json
{"type": "auth", "user": "<user>", "password": "<password>"}
```

Isso evita incluir credenciais em parâmetros de consulta na URL ou em cabeçalhos `Authorization` anexados à solicitação de upgrade, onde elas poderiam acabar no histórico do navegador, nos logs de acesso do servidor e nos logs do proxy reverso. Os parâmetros de URL, o HTTP Basic e os cabeçalhos `X-ClickHouse-User`/`X-ClickHouse-Key` na solicitação de upgrade intencionalmente **não** são considerados pelo `/webterminal`.

Credenciais inválidas fazem o servidor fechar o WebSocket com o código `1008`; a UI do navegador solicita as credenciais novamente.

<div id="session">
  ## Como é a sessão
</div>

Após a autenticação, o servidor executa o `clickhouse-client` conectado a um pseudoterminal e encaminha sua entrada e saída por WebSocket. A sessão oferece a experiência completa do `clickhouse-client`, incluindo:

* Realce de sintaxe.
* Autocompletar.
* Consultas em várias linhas.
* Histórico de comandos (armazenado no servidor durante a sessão).

O terminal usa [xterm.js](https://xtermjs.org/) para renderização. Todos os recursos são servidos pelo próprio binário do ClickHouse — nenhum CDN de terceiros é carregado.

<div id="play-integration">
  ## Integração com `/play`
</div>

A UI Web SQL [`/play`](/pt-BR/interfaces/http) incorpora o terminal web como um painel acoplável. Ative-o ou desative-o pelo ícone do terminal na barra lateral ou pressione a tecla `~` quando o editor de consultas estiver vazio. A página `/play` detecta a disponibilidade de `/webterminal` ao carregar e oculta os controles do terminal quando o endpoint não está disponível (por exemplo, quando `enable_webterminal` está definido como `false`).

<div id="security">
  ## Considerações de segurança
</div>

O terminal web expõe uma sessão interativa semelhante a um shell para qualquer pessoa que consiga se autenticar no endpoint HTTP do ClickHouse, portanto as mesmas ressalvas aplicáveis ao protocolo HTTP também se aplicam aqui:

* Sempre disponibilize `/webterminal` por HTTPS em ambientes não confiáveis para proteger as credenciais e o tráfego da sessão.
* Restrinja o acesso no nível da rede (firewall, proxy reverso ou a configuração `listen_host`) da mesma forma que você restringe o acesso ao protocolo HTTP.
* O endpoint valida o cabeçalho `Origin` em relação ao `Host` para mitigar o sequestro de WebSocket entre origens; configure os proxies reversos adequadamente se fizer a terminação de TLS externamente.
* Por trás de um proxy reverso com terminação de TLS, a conexão entre o proxy e o ClickHouse é `http` simples, embora o navegador use `https`, então a verificação estrita de mesma origem rejeitaria conexões legítimas. Para essas implantações, defina `webterminal_allowed_origins` como uma lista, separada por vírgulas, de origens completas autorizadas a abrir sessões WebSocket; quando essa configuração não estiver vazia, ela substituirá a verificação padrão de mesma origem. Exemplo: `<webterminal_allowed_origins>https://example.com,https://app.example.com:8443</webterminal_allowed_origins>`.

O handler também impõe conformidade com o protocolo WebSocket de acordo com a RFC 6455: frames de cliente sem máscara, opcodes reservados, frames de controle grandes demais ou fragmentados e bits RSV reservados são rejeitados com códigos de fechamento por erro de protocolo.

<div id="platform">
  ## Disponibilidade da plataforma
</div>

O handler é compilado em todas as plataformas compatíveis com o ClickHouse. A camada de pseudoterminal usada pelo executor embutido do `clickhouse-client` é implementada com base em primitivas POSIX portáveis (`posix_openpt`/`grantpt`/`unlockpt`), com um caminho específico para Linux que usa `ptsname_r`, que é thread-safe. Os links para `/webterminal` na página inicial do ClickHouse e em `/play` são ocultados automaticamente quando o endpoint não está disponível (por exemplo, quando `enable_webterminal` é definido como `false`).