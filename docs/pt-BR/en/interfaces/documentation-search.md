---
description: 'Documentação da interface web de busca da documentação integrada, disponibilizada no caminho `/docs` da interface HTTP e baseada na tabela `system.documentation`'
sidebar_label: 'Busca na documentação'
sidebar_position: 23
slug: /interfaces/documentation-search
title: 'Busca na documentação'
doc_type: 'reference'
---

A página de busca da documentação é uma pequena interface web autônoma para busca instantânea na documentação de referência embutida. Ela fica disponível no caminho `/docs` em qualquer porta HTTP do ClickHouse.

Acesse `/docs` em qualquer porta HTTP do ClickHouse (por exemplo, `http://localhost:8123/docs`) para abri-la.

<div id="what-it-does">
  ## O que ela faz
</div>

A página consulta a tabela [`system.documentation`](/pt-BR/operations/system-tables/documentation) via HTTP enquanto você digita e renderiza o Markdown da entidade selecionada. Como lê `system.documentation`, ela abrange todas as entidades que essa tabela expõe — funções, funções de agregação, funções de tabela, motores de tabela, motores de banco de dados, tipos de dados, configurações, formatos, codecs de compressão, eventos de perfil, métricas, as próprias tabelas de sistema e muito mais — e sempre corresponde à documentação embutida no servidor em execução.

Digite na caixa de busca e as correspondências aparecem em uma lista com cores por tipo; ao selecionar uma correspondência, a documentação dela é renderizada. A renderização inclui:

* um link com ícone de lápis ao lado do título da entidade, que abre seu arquivo de origem no GitHub, obtido da coluna `source` de `system.documentation`;
* realce de sintaxe de ClickHouse SQL nos blocos de código, usando o mesmo lexer embutido (`Lexer.wasm`) da UI [`/play`](/pt-BR/interfaces/http);
* fórmulas TeX via [KaTeX](https://katex.org/) (por exemplo, a fórmula na página `corr`);
* admonições `:::note`/`:::tip`/…, âncoras de cabeçalho com links compartilháveis e um botão flutuante &quot;Copy&quot; nos blocos de código;
* links relativos resolvidos no app para outra entidade documentada, quando ela existe; caso contrário, para `https://clickhouse.com/docs`; referências &quot;Related&quot; e &quot;Alias of&quot; se tornam links no app.

O termo de busca atual, a entidade aberta e a seção são espelhados no fragmento da URL, para que uma página ou seção específica possa ser vinculada diretamente e restaurada pela navegação de voltar/avançar do navegador. Um alternador de tema claro/escuro (com autodetecção) corresponde ao `/play`.

<div id="connecting">
  ## Conexão
</div>

O cabeçalho tem campos de `URL`, `user` e `password`, exatamente como em `/play`. Quando a página é servida pelo ClickHouse, a `URL` usa por padrão a origem atual; quando a página é aberta como um arquivo local, o padrão passa a ser `http://localhost:8123/`, para que a página também possa ser aberta localmente e usada com um servidor remoto. O cache de nomes de links cruzados é reconstruído automaticamente quando a conexão muda.

<div id="assets">
  ## Recursos
</div>

Todos os recursos — incluindo o renderizador de Markdown ([Marked](https://marked.js.org/)), o renderizador matemático (KaTeX, com suas fontes) e o analisador léxico de SQL — são servidos pelo próprio binário do ClickHouse quando a página é disponibilizada por HTTP. Nenhuma CDN de terceiros é carregada na origem HTTP do ClickHouse, portanto a página é autocontida, funciona offline e não executa código de rede de terceiros junto com as credenciais que processa.

<div id="security">
  ## Considerações de segurança
</div>

A página faz consultas ao endpoint HTTP do ClickHouse com as credenciais informadas no cabeçalho, portanto as mesmas ressalvas aplicáveis ao protocolo HTTP também se aplicam aqui:

* Sempre disponibilize `/docs` via HTTPS em ambientes não confiáveis para proteger as credenciais.
* Restrinja o acesso no nível da rede (firewall, proxy reverso ou a configuração `listen_host`) da mesma forma que você restringe o acesso ao protocolo HTTP.

`system.documentation` contém apenas a documentação de referência estática embutida no servidor, portanto a página não expõe nenhum dado das suas tabelas.