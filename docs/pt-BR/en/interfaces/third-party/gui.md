---
description: 'Lista de ferramentas e aplicativos de interface gráfica de terceiros para trabalhar com o ClickHouse'
sidebar_label: 'Interfaces visuais'
sidebar_position: 28
slug: /interfaces/third-party/gui
title: 'Interfaces visuais de desenvolvedores terceirizados'
doc_type: 'reference'
---

<div id="open-source">
  ## Código aberto
</div>

<div id="agx">
  ### agx
</div>

[agx](https://github.com/agnosticeng/agx) é um aplicativo para desktop criado com Tauri e SvelteKit que oferece uma interface moderna para explorar e consultar dados usando o engine de banco de dados embutido do ClickHouse (chdb).

* Aproveite o chdb ao executar o aplicativo nativo.
* Pode se conectar a uma instância do ClickHouse ao executar a versão web.
* Editor Monaco para que você se sinta em casa.
* Múltiplas visualizações de dados em constante evolução.

<div id="ch-ui">
  ### ch-ui
</div>

[ch-ui](https://github.com/caioricciuti/ch-ui) é uma interface simples em React.js para bancos de dados ClickHouse, desenvolvida para executar consultas e visualizar dados. Criada com React e o cliente ClickHouse para web, ela oferece uma UI elegante e intuitiva para facilitar as interações com o banco de dados.

Recursos:

* Integração com ClickHouse: gerencie conexões e execute consultas com facilidade.
* Gerenciamento responsivo de abas: gerencie dinamicamente várias abas, como abas de consulta e de tabela.
* Otimizações de desempenho: utiliza IndexedDB para gerenciamento eficiente de cache e estado.
* Armazenamento local de dados: todos os dados são armazenados localmente no navegador, garantindo que nenhum dado seja enviado para outro local.

<div id="chartdb">
  ### ChartDB
</div>

[ChartDB](https://chartdb.io) é uma ferramenta gratuita e de código aberto para visualizar e modelar esquemas de banco de dados, incluindo o ClickHouse, a partir de uma única consulta. Desenvolvida com React, ela oferece uma experiência fluida e fácil de usar, sem exigir credenciais de banco de dados nem cadastro para começar.

Recursos:

* Visualização de esquema: Importe e visualize instantaneamente seu esquema do ClickHouse, incluindo diagramas ER com visões materializadas e visões padrão, mostrando referências a tabelas.
* Exportação de DDL com IA: Gere scripts DDL com facilidade para melhorar o gerenciamento e a documentação do esquema.
* Suporte a vários dialetos SQL: Compatível com uma variedade de dialetos SQL, o que a torna versátil para diversos ambientes de banco de dados.
* Sem necessidade de cadastro ou credenciais: Todas as funcionalidades podem ser acessadas diretamente no navegador, mantendo a experiência simples e segura.

[Código-fonte do ChartDB](https://github.com/chartdb/chartdb).

<div id="datastoria">
  ### DataStoria
</div>

[DataStoria](https://github.com/FrankChen021/datastoria) é um aplicativo de console web com IA que gerencia vários clusters do ClickHouse em um só lugar.

Recursos:

* **Inteligência com IA**: Use linguagem natural para explorar dados, otimizar e corrigir consultas SQL e visualizar seus dados.
* **Integração oficial com o ClickHouse Agent Skills**: Aproveite as [melhores práticas oficiais](https://github.com/ClickHouse/agent-skills) para pedir à IA otimizações e sugestões para o banco de dados.
* **Diagnóstico inteligente de erros**: Identifique erros de sintaxe instantaneamente com destaque preciso de linha e coluna e receba sugestões de correção com IA em um clique.
* **Inspeção de tabelas de sistema**: Aprofunde-se em `system.query_log`, `system.query_views_log`, `system.zookeeper`, `system.ddl_distributed_queue`, `system.part_log` e `system.processes` com um dashboard avançado de visualização e filtros para entender rapidamente seu cluster.
* **Explain com um clique**: Entenda instantaneamente os planos de execução de consultas com visualizações do AST e do pipeline.
* **Grafo de dependências**: Visualize relacionamentos entre tabelas e acompanhe os fluxos de dados por visões materializadas, tabelas distribuídas e sistemas externos.
* **Monitoramento de cluster**: Monitore todos os nós com métricas em tempo real, operações de merge, status de replicação, desempenho de consultas e muito mais.
* **Privacidade e segurança**: Todas as consultas SQL são executadas diretamente do seu navegador para o servidor ClickHouse, garantindo privacidade total.

[Documentação do DataStoria](https://docs.datastoria.app).

<div id="datapup">
  ### DataPup
</div>

[DataPup](https://github.com/DataPupOrg/DataPup) é um cliente de banco de dados moderno, multiplataforma, com assistência de IA e suporte nativo ao ClickHouse.

Recursos:

* Assistência a consultas SQL com IA e sugestões inteligentes
* Suporte nativo a conexões com ClickHouse, com gerenciamento seguro de credenciais
* Interface elegante e acessível, com vários temas (claro, escuro e variantes coloridas)
* Filtragem e exploração avançadas de resultados de consulta
* Suporte multiplataforma (macOS, Windows, Linux)
* Desempenho rápido e responsivo
* Código aberto e licenciado sob a licença MIT

<div id="dory">
  ### Dory
</div>

[Dory](https://github.com/dorylab/dory) workspace de SQL projetado para IA, com suporte de primeira linha ao ClickHouse e IA integrada.

Recursos:

* Copiloto de IA para geração, explicação e depuração de SQL
* Gerenciamento e consulta de vários clusters ClickHouse em um workspace unificado
* Autocompletar de SQL com reconhecimento de esquema e workspace de consultas com várias abas
* Exploração interativa de resultados de consultas com filtragem e visualização
* Resumos de tabelas com IA para entender conjuntos de dados
* Conexões diretas com o ClickHouse com suporte a túnel SSH
* Interface moderna e amigável para desenvolvedores, com suporte a temas, incluindo claro e escuro
* Aplicativo de desktop multiplataforma (macOS, Windows, Linux) e suporte a Docker
* Código aberto e licenciado sob a licença MIT

<div id="clickhouse-schemaflow-visualizer">
  ### Visualizador de Fluxo de Esquema do ClickHouse
</div>

[ClickHouse Schema Flow Visualizer](https://github.com/FulgerX2007/clickhouse-schemaflow-visualizer) é uma aplicação web de código aberto para visualizar relacionamentos entre tabelas do ClickHouse.
Ela se conecta a uma instância do ClickHouse, analisa os metadados de `system.tables` (tipos de engine, dependências, instruções SELECT de visão materializada) e renderiza diagramas interativos de fluxo de dados no nível da tabela, juntamente com relacionamentos no nível da coluna, com a expressão de transformação identificada em cada aresta. Os diagramas são organizados com Dagre e renderizados como SVG inline simples — nenhum runtime de criação de diagramas no lado do cliente é carregado.

Recursos:

* Navegue por bancos de dados e tabelas do ClickHouse com uma barra lateral intuitiva
* Visualização de Fluxo de Dados: sources upstream e visões materializadas downstream no nível da tabela
* Visualização de Relacionamentos: mapeamento no nível da coluna com a expressão de transformação analisada em cada aresta (por exemplo, `toStartOfHour(scheduled_departure)`, `avgState(delay_minutes)`)
* Ícones e codificação por cores com reconhecimento de engine para `MergeTree`, `Replicated*`, `Distributed`, `MaterializedView` e `Dictionary`
* Clique em uma coluna na visualização de Relacionamentos para destacar todo o caminho dos dados ao longo da pipeline
* Filtro dinâmico na barra lateral e uma paleta de comandos `Ctrl+K` / `⌘K` para ir para qualquer tabela, coluna ou engine
* Sobreposição opcional de metadados mostrando contagens de linhas e tamanho em disco por tabela
* Exporte o diagrama atual como um arquivo HTML autônomo
* Conexão TLS com o ClickHouse, com opção de ignorar a verificação e usar CA / certificados de client personalizados

[ClickHouse Schema Flow Visualizer - código-fonte](https://github.com/FulgerX2007/clickhouse-schemaflow-visualizer)

<div id="tabix">
  ### Tabix
</div>

Interface web do ClickHouse no projeto [Tabix](https://github.com/tabixio/tabix).

Recursos:

* Funciona com o ClickHouse diretamente no navegador, sem necessidade de instalar software adicional.
* Editor de consultas com realce de sintaxe.
* Autocompletar de comandos.
* Ferramentas para análise visual da execução de consultas.
* Opções de cores.

[Documentação do Tabix](https://tabix.io/doc/).

<div id="houseops">
  ### HouseOps
</div>

[HouseOps](https://github.com/HouseOps/HouseOps) é uma UI/IDE para OSX, Linux e Windows.

Recursos:

* Construtor de consultas com realce de sintaxe. Veja a resposta em formato de tabela ou na visualização JSON.
* Exporte os resultados da consulta como CSV ou JSON.
* Lista de processos com descrições. Modo de edição. Possibilidade de interromper (`KILL`) um processo.
* Diagrama do banco de dados. Mostra todas as tabelas e suas colunas com informações adicionais.
* Visualização rápida do tamanho da coluna.
* Configuração do servidor.

Os seguintes recursos estão previstos:

* Gerenciamento de banco de dados.
* Gerenciamento de usuários.
* Análise de dados em tempo real.
* Monitoramento de cluster.
* Gerenciamento de cluster.
* Monitoramento de tabelas Replicated e Kafka.

<div id="lighthouse">
  ### LightHouse
</div>

[LightHouse](https://github.com/VKCOM/lighthouse) é uma interface web leve para o ClickHouse.

Recursos:

* Lista de tabelas com filtragem e metadados.
* Pré-visualização de tabelas com filtragem e ordenação.
* Execução de consultas em modo somente leitura.

<div id="redash">
  ### Redash
</div>

[Redash](https://github.com/getredash/redash) é uma plataforma de visualização de dados.

Com suporte a várias fontes de dados, incluindo o ClickHouse, o Redash pode combinar os resultados de consultas de diferentes fontes de dados em um único conjunto de dados final.

Recursos:

* Editor de consultas poderoso.
* Explorador de banco de dados.
* Ferramenta de visualização que permite representar dados de diferentes formas.

<div id="grafana">
  ### Grafana
</div>

[Grafana](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) é uma plataforma de monitoramento e visualização.

&quot;O Grafana permite consultar, visualizar, criar alertas e entender suas métricas, não importa onde elas estejam armazenadas. Crie, explore e compartilhe dashboards com sua equipe e promova uma cultura orientada por dados. Uma plataforma confiável e querida pela comunidade&quot; — grafana.com.

O plugin de fonte de dados do ClickHouse oferece suporte ao ClickHouse como banco de dados de back-end.

<div id="qryn">
  ### qryn
</div>

[qryn](https://metrico.in) é uma stack de observabilidade poliglota e de alto desempenho para ClickHouse *(anteriormente cLoki)*, com integrações nativas com o Grafana que permitem aos usuários fazer ingestão e analisar logs, métricas e traces de telemetria de qualquer agente compatível com Loki/LogQL, Prometheus/PromQL, OTLP/Tempo, Elastic, InfluxDB e muitos outros.

Funcionalidades:

* UI Explore integrada e CLI LogQL para consultar, extrair e visualizar dados
* Suporte nativo às APIs do Grafana para consulta, processamento, ingestão, rastreamento e alertas, sem plugins
* Pipeline avançado para pesquisar, filtrar e extrair dinamicamente dados de logs, eventos, traces e muito mais
* APIs de ingestão e envio compatíveis de forma transparente com LogQL, PromQL, InfluxDB, Elastic e muitos outros
* Pronto para uso com agentes como Promtail, Grafana-Agent, Vector, Logstash, Telegraf e muitos outros

<div id="dbeaver">
  ### DBeaver
</div>

[DBeaver](https://dbeaver.io/) - cliente universal de banco de dados para desktop com suporte a ClickHouse.

Recursos:

* Desenvolvimento de consultas com destaque de sintaxe e autocompletar.
* Lista de tabelas com filtros e busca de metadados.
* Visualização prévia dos dados da tabela.
* Busca de texto completo.

Por padrão, o DBeaver não se conecta usando uma sessão (a CLI, por exemplo, usa). Se você precisar de suporte a sessão (por exemplo, para definir configurações para a sua sessão), edite as propriedades de conexão do driver e defina `session_id` como uma string aleatória (ele usa a conexão HTTP nos bastidores). Assim, você poderá usar qualquer configuração na janela de consulta.

<div id="clickhouse-cli">
  ### clickhouse-cli
</div>

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli) é um command-line client alternativo para o ClickHouse, escrito em Python 3.

Recursos:

* Autocompletar.
* Realce de sintaxe para as consultas e a saída de dados.
* Suporte a pager para a saída de dados.
* Comandos personalizados no estilo do PostgreSQL.

<div id="clickhouse-flamegraph">
  ### clickhouse-flamegraph
</div>

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph) é uma ferramenta especializada para visualizar o `system.trace_log` na forma de [flamegraph](http://www.brendangregg.com/flamegraphs.html).

<div id="clickhouse-plantuml">
  ### clickhouse-plantuml
</div>

[cickhouse-plantuml](https://pypi.org/project/clickhouse-plantuml/) é um script para gerar diagramas [PlantUML](https://plantuml.com/) dos esquemas das tabelas.

<div id="clickhouse-table-graph">
  ### Grafo de tabelas do ClickHouse
</div>

[ClickHouse table graph](https://github.com/mbaksheev/clickhouse-table-graph) é uma ferramenta CLI simples para visualizar dependências entre tabelas do ClickHouse. Essa ferramenta obtém as conexões entre tabelas a partir da tabela `system.tables` e gera um fluxograma de dependências no formato [mermaid](https://mermaid.js.org/syntax/flowchart.html). Com essa ferramenta, você pode visualizar facilmente as dependências entre tabelas e entender o fluxo de dados no seu banco de dados ClickHouse. Graças ao mermaid, o fluxograma gerado fica visualmente atraente e pode ser facilmente adicionado à sua documentação em Markdown.

<div id="xeus-clickhouse">
  ### xeus-clickhouse
</div>

[xeus-clickhouse](https://github.com/wangfenjin/xeus-clickhouse) é um kernel do Jupyter para ClickHouse que permite consultar dados do ClickHouse usando SQL no Jupyter.

<div id="mindsdb">
  ### MindsDB Studio
</div>

[MindsDB](https://mindsdb.com/) é uma camada de IA de código aberto para bancos de dados, incluindo o ClickHouse, que permite desenvolver, treinar e implantar com facilidade modelos de machine learning de última geração. O MindsDB Studio(GUI) permite treinar novos modelos com base em dados do banco de dados, interpretar previsões feitas pelo modelo, identificar possíveis vieses nos dados e avaliar e visualizar a precisão do modelo usando a funcionalidade de IA explicável para adaptar e ajustar seus modelos de machine learning mais rapidamente.

<div id="dbm">
  ### DBM
</div>

[DBM](https://github.com/devlive-community/dbm) é uma ferramenta visual de gerenciamento para ClickHouse!

Funcionalidades:

* Suporta histórico de consultas (paginação, limpar tudo etc.)
* Suporta consultas com cláusulas SQL selecionadas
* Suporta encerramento de consultas
* Suporta gerenciamento de tabelas (metadados, exclusão, visualização prévia)
* Suporta gerenciamento de bancos de dados (exclusão, criação)
* Suporta consultas personalizadas
* Suporta gerenciamento de várias fontes de dados (teste de conexão, monitoramento)
* Suporta monitoramento (processador, conexão, consulta)
* Suporta migração de dados

<div id="bytebase">
  ### Bytebase
</div>

[Bytebase](https://bytebase.com) é uma ferramenta web de código aberto para alterações de esquema e controle de versão voltada para equipes. Ela oferece suporte a vários bancos de dados, incluindo o ClickHouse.

Recursos:

* Revisão de esquema entre desenvolvedores e DBAs.
* Database-as-Code, com controle de versão do esquema em VCS como o GitLab e acionamento da implantação após o commit do código.
* Implantação simplificada com política por ambiente.
* Histórico completo de migrações.
* Detecção de esquema drift.
* Backup e restauração.
* RBAC.

<div id="zeppelin-interpreter-for-clickhouse">
  ### Zeppelin-Interpreter-for-ClickHouse
</div>

[Zeppelin-Interpreter-for-ClickHouse](https://github.com/SiderZhang/Zeppelin-Interpreter-for-ClickHouse) é um interpretador do [Zeppelin](https://zeppelin.apache.org) para ClickHouse. Em comparação com o interpretador JDBC, ele pode oferecer um controle melhor de timeout para queries de longa execução.

<div id="clickcat">
  ### ClickCat
</div>

[ClickCat](https://github.com/clickcat-project/ClickCat) é uma interface amigável que permite buscar, explorar e visualizar seus dados no ClickHouse.

Recursos:

* Um editor SQL online que pode executar seu código SQL sem precisar instalar nada.
* Você pode acompanhar todos os processos e mutações. Para os processos que ainda não terminaram, é possível encerrá-los pela interface.
* As métricas incluem análise de cluster, análise de dados e análise de consultas.

<div id="clickvisual">
  ### ClickVisual
</div>

[ClickVisual](https://clickvisual.net/) O ClickVisual é uma plataforma leve e de código aberto para consulta, análise e visualização de alertas de logs.

Recursos:

* Oferece suporte à criação, com um clique, de bibliotecas de análise de logs
* Oferece suporte ao gerenciamento da configuração da coleta de logs
* Oferece suporte à configuração de índices definida pelo usuário
* Oferece suporte à configuração de alertas
* Oferece suporte à granularidade de permissões até o nível de biblioteca e tabela

<div id="clickmate">
  ### ClickHouse-Mate
</div>

[ClickHouse-Mate](https://github.com/metrico/clickhouse-mate) é um cliente web em Angular + interface de usuário para pesquisar e explorar dados no ClickHouse.

Recursos:

* Autocompletar para consultas em ClickHouse SQL
* Navegação rápida na árvore de bancos de dados e tabelas
* Filtragem e ordenação avançadas dos resultados
* Documentação embutida do ClickHouse SQL
* Predefinições e histórico de consultas
* 100% no navegador, sem servidor/backend

O cliente está disponível para uso imediato via GitHub Pages: https://metrico.github.io/clickhouse-mate/

<div id="uptrace">
  ### Uptrace
</div>

[Uptrace](https://github.com/uptrace/uptrace) é uma ferramenta de APM que fornece rastreamento distribuído e métricas com OpenTelemetry e ClickHouse.

Recursos:

* [Tracing com OpenTelemetry](https://uptrace.dev/opentelemetry/distributed-tracing.html), métricas e logs.
* Notificações por e-mail/Slack/PagerDuty usando AlertManager.
* Linguagem de consulta semelhante a SQL para agregar spans.
* Linguagem no estilo do PromQL para consultar métricas.
* Dashboards de métricas pré-configurados.
* Vários usuários/projetos via configuração YAML.

<div id="clickhouse-monitoring">
  ### clickhouse-monitoring
</div>

[clickhouse-monitoring](https://github.com/duyet/clickhouse-monitoring) é um dashboard simples em Next.js que usa as tabelas `system.*` para ajudar a monitorar e fornecer uma visão geral do seu cluster ClickHouse.

Funcionalidades:

* Monitor de consultas: consultas atuais, histórico de consultas, recursos das consultas (memória, partes lidas, file&#95;open, ...), consultas mais caras, tabelas ou colunas mais usadas etc.
* Monitor do cluster: uso total de memória/CPU, fila distribuída, configurações globais, configurações do MergeTree, métricas etc.
* Informações sobre tabelas e partes: tamanho, contagem de linhas, compressão, tamanho das partes etc., com detalhamento no nível de coluna.
* Ferramentas úteis: exploração de dados do ZooKeeper, EXPLAIN de consultas, encerramento de consultas etc.
* Gráficos de métricas para visualização: consultas e uso de recursos, número de merges/mutations, desempenho de merge, desempenho de consultas etc.

<div id="ckibana">
  ### CKibana
</div>

[CKibana](https://github.com/TongchengOpenSource/ckibana) é um serviço leve que permite pesquisar, explorar e visualizar dados do ClickHouse com facilidade usando a UI nativa do Kibana.

Recursos:

* Traduz solicitações de gráficos da UI nativa do Kibana para a sintaxe de consulta do ClickHouse.
* Oferece suporte a recursos avançados, como amostragem e armazenamento em cache, para melhorar o desempenho das consultas.
* Minimiza a curva de aprendizado dos usuários após a migração do ElasticSearch para o ClickHouse.

<div id="telescope">
  ### Telescope
</div>

[Telescope](https://iamtelescope.net/) é uma interface web moderna para explorar logs armazenados no ClickHouse. Ela oferece uma UI amigável para consultar, visualizar e gerenciar dados de log com controle de acesso granular.

Recursos:

* UI limpa e responsiva, com filtros avançados e seleção de campos personalizável.
* Sintaxe FlyQL para filtragem de logs intuitiva e expressiva.
* Gráfico baseado em tempo com suporte a group-by, incluindo campos JSON aninhados, Map e Array.
* Suporte opcional a consultas `WHERE` em SQL puro para filtragem avançada (com verificações de permissão).
* Views salvas: permitem salvar e compartilhar configurações personalizadas da UI para consultas e layout.
* Controle de Acesso Baseado em Funções (RBAC) e integração com autenticação do GitHub.
* Nenhum agente ou componente extra é necessário no lado do ClickHouse.

[Código-fonte do Telescope](https://github.com/iamtelescope/telescope) · [Demo ao vivo](https://demo.iamtelescope.net)

<div id="clicklens">
  ### ClickLens
</div>

[ClickLens](https://ntk148v.github.io/clicklens/) é uma interface web moderna, poderosa e fácil de usar para gerenciar e monitorar bancos de dados ClickHouse. Ele oferece um conjunto abrangente de ferramentas para que desenvolvedores, analistas e administradores interajam com seus clusters ClickHouse de forma eficiente. O ClickHouse é um banco de dados analítico incrível, mas gerenciá-lo via CLI ou com ferramentas básicas pode ser desafiador. O ClickLens preenche essa lacuna ao oferecer:

* Discover - Exploração de dados flexível, no estilo do Kibana, para qualquer tabela
* SQL Console - Escreva, execute e analise consultas com realce de sintaxe e resultados em streaming
* Monitoramento em tempo real - Acompanhe a integridade do seu cluster, o desempenho das consultas e o uso de recursos
* Esquema Explorer - Navegue por bancos de dados, tabelas, colunas, partes e muito mais
* Controle de acesso - Gerencie usuários e funções diretamente pela UI
* RBAC nativo - Suas permissões na UI são derivadas diretamente dos grants do ClickHouse

[Código-fonte do ClickLens](https://github.com/ntk148v/clicklens)

<div id="chouse-ui">
  ### CHouse UI
</div>

[CHouse UI](https://chouse-ui.com) é uma interface web de código aberto e auto-hospedada para ClickHouse, criada para **equipes que operam ClickHouse em produção**. A maioria das ferramentas resolve bem uma parte — um workspace de consultas, um dashboard, um assistente de IA, um monitor de cluster; o CHouse UI é a *combinação*: uma camada de acesso para equipes, junto com monitoramento de frota multicluster e um SRE de IA autônomo, somente leitura. Ao contrário de clientes que exigem credenciais diretas do banco de dados, ele as armazena criptografadas no servidor e controla o acesso com sua própria camada de **Controle de Acesso Baseado em Funções (RBAC)**, para que o navegador nunca tenha acesso à senha do ClickHouse.

Recursos:

* **Acesso de equipe e segurança** - RBAC no nível da aplicação (funções predefinidas + funções personalizadas, regras granulares de acesso a dados por banco de dados/tabela), logs de auditoria com contexto real de sessão e credenciais criptografadas no servidor com AES-256-GCM.
* **Frota multicluster** - Acompanhe todos os clusters configurados em um único painel (status, memória, consultas ativas, exceções, minigráficos de tendência), com cada cartão fazendo polling de forma independente, apoiado por um poller de snapshots no backend.
* **Chouse AI — Fleet Doctor** - Um SRE de IA autônomo e somente leitura: ele varre a frota com uma ferramenta `SELECT` protegida apenas para `system.*` (ClickHouse `readonly=1`), identifica as causas raiz e gera um relatório estruturado com uma análise aprofundada de consultas pesadas e reescritas sugeridas. Ele nunca altera o cluster.
* **IA nas abas de monitoramento** - &quot;Optimize with Chouse AI&quot; em uma linha de Query Logs (reescrita + estimativa `EXPLAIN` antes→depois + abrir no workspace SQL), além de &quot;Diagnose&quot; com um clique em uma linha de `system.errors` ou em uma entrada do log de partes.
* **Alertas por limite** - Regras para % de memória do nó, memória por consulta e consultas de longa duração enviadas para Slack e e-mail — com uma análise autônoma da causa raiz anexada quando o limite é excedido.
* **Workspace completo** - Editor SQL Monaco, explorador de esquema, visualização de consultas em tempo real com suporte para kill, monitoramento nativo do ClickHouse (detalhamento de memória, partes/merges, atraso de réplica, percentis de latência) e importação/exportação de dados.

Código aberto (Apache 2.0), com foco em on-premises — todos os recursos já vêm incluídos, sem nível pago.

[CHouse UI Código-fonte](https://github.com/daun-gatal/chouse-ui)

<div id="clickhouse-flow">
  ### clickhouse-flow
</div>

[clickhouse-flow](https://github.com/MikeAmputer/clickhouse-flow) é uma ferramenta de código aberto para visualizar fluxos de dados e dependências entre tabelas, visões e visões materializadas no ClickHouse.

Recursos:

* Cria automaticamente um grafo do esquema a partir dos metadados do ClickHouse.
* Visualiza fluxos de dados por meio de visões materializadas.
* UI interativa para explorar a estrutura do esquema.
* Exporta diagramas em PDF ou SVG para documentação e compartilhamento.
* Implantação com Docker para configuração rápida em ambientes de desenvolvimento.

<div id="commercial">
  ## Comercial
</div>

<div id="datagrip">
  ### DataGrip
</div>

[DataGrip](https://www.jetbrains.com/datagrip/) é uma IDE de banco de dados da JetBrains com suporte dedicado ao ClickHouse. Também vem embutido em outras ferramentas baseadas em IntelliJ, como PyCharm, IntelliJ IDEA, GoLand, PhpStorm e outras.

Recursos:

* Autocompletar de código muito rápido.
* Realce de sintaxe do ClickHouse.
* Suporte a recursos específicos do ClickHouse, por exemplo, colunas aninhadas e motores de tabela.
* Editor de dados.
* Refatorações.
* Busca e navegação.

<div id="yandex-datalens">
  ### Yandex DataLens
</div>

[Yandex DataLens](https://yandex.cloud/en/services/datalens) é um serviço de visualização de dados e analytics.

Recursos:

* Ampla variedade de visualizações disponíveis, desde gráficos de barras simples até dashboards complexos.
* Os dashboards podem ser disponibilizados publicamente.
* Suporte a várias fontes de dados, incluindo o ClickHouse.
* Armazenamento de dados materializados com base no ClickHouse.

O DataLens está [disponível gratuitamente](https://yandex.cloud/en/docs/datalens/pricing) para projetos de baixa carga, inclusive para uso comercial.

* [Documentação do DataLens](https://yandex.cloud/en/docs/datalens/).
* [Tutorial](https://yandex.cloud/en/docs/solutions/datalens/data-from-ch-visualization) sobre visualização de dados de um banco de dados ClickHouse.

<div id="holistics-software">
  ### Holistics Software
</div>

[Holistics](https://www.holistics.io/) é uma plataforma de dados full-stack e uma ferramenta de inteligência de negócios.

Recursos:

* Agendamentos automatizados de relatórios por email, Slack e Google Sheets.
* Editor SQL com visualizações, controle de versão, preenchimento automático, componentes de consulta reutilizáveis e filtros dinâmicos.
* Analytics embutido de relatórios e dashboards via iframe.
* Recursos de preparação de dados e ETL.
* Suporte à modelagem de dados em SQL para o mapeamento relacional dos dados.

<div id="looker">
  ### Looker
</div>

[Looker](https://looker.com) é uma plataforma de dados e ferramenta de inteligência de negócios com suporte a mais de 50 dialetos de banco de dados, incluindo ClickHouse. O Looker está disponível como plataforma SaaS e auto-hospedado. Os usuários podem usar o Looker no navegador para explorar dados, criar visualizações e dashboards, agendar relatórios e compartilhar seus insights com colegas. O Looker oferece um conjunto robusto de ferramentas para incorporar esses recursos a outras aplicações e uma API
para integrar dados a outras aplicações.

Recursos:

* Desenvolvimento fácil e ágil usando LookML, uma linguagem com suporte a
  [modelagem de dados](https://looker.com/platform/data-modeling) com curadoria para atender autores de relatórios e usuários finais.
* Poderosa integração de workflow por meio de [Data Actions](https://looker.com/platform/actions) do Looker.

[Como configurar o ClickHouse no Looker.](https://docs.looker.com/setup-and-management/database-config/clickhouse)

<div id="seektable">
  ### SeekTable
</div>

[SeekTable](https://www.seektable.com) é uma ferramenta de BI self-service para exploração de dados e relatórios operacionais. Ela está disponível tanto como um serviço em nuvem quanto em uma versão auto-hospedada. Os relatórios do SeekTable podem ser embutidos em qualquer aplicativo web.

Recursos:

* Criador de relatórios amigável para usuários de negócio.
* Parâmetros avançados de relatório para filtragem em SQL e personalizações de consulta específicas do relatório.
* Pode se conectar ao ClickHouse tanto com um endpoint TCP/IP nativo quanto com uma interface HTTP(S) (2 drivers diferentes).
* É possível usar todos os recursos do dialeto SQL do ClickHouse nas definições de dimensões/medidas.
* [Web API](https://www.seektable.com/help/web-api-integration) para geração automatizada de relatórios.
* Oferece suporte ao fluxo de desenvolvimento de relatórios com [backup/restauração](https://www.seektable.com/help/self-hosted-backup-restore) dos dados da conta; a configuração de modelos de dados (cubos) / relatórios é um XML legível por pessoas e pode ser armazenada em um sistema de controle de versão.

O SeekTable é [gratuito](https://www.seektable.com/help/cloud-pricing) para uso pessoal/individual.

[Como configurar a conexão com o ClickHouse no SeekTable.](https://www.seektable.com/help/clickhouse-pivot-table)

<div id="chadmin">
  ### Chadmin
</div>

[Chadmin](https://github.com/bun4uk/chadmin) é uma UI simples na qual você pode visualizar as consultas em execução no seu cluster do ClickHouse, ver informações sobre elas e encerrá-las, se quiser.

<div id="tablum_io">
  ### TABLUM.IO
</div>

[TABLUM.IO](https://tablum.io/) — uma ferramenta online de consultas e analytics para ETL e visualização. Ela permite conectar-se ao ClickHouse, consultar dados por meio de um console SQL versátil, bem como carregar dados de arquivos estáticos e serviços de terceiros. O TABLUM.IO também permite visualizar os resultados dos dados em gráficos e tabelas.

Recursos:

* ETL: carregamento de dados de bancos de dados populares, arquivos locais e remotos e invocações de API.
* Console SQL versátil com destaque de sintaxe e construtor visual de consultas.
* Visualização de dados em gráficos e tabelas.
* Materialização de dados e subconsultas.
* Relatórios de dados para Slack, Telegram ou e-mail.
* Pipelines de dados por meio de API proprietária.
* Exportação de dados nos formatos JSON, CSV, SQL e HTML.
* Interface web.

O TABLUM.IO pode ser executado como uma solução auto-hospedado (como uma imagem Docker) ou na nuvem.
Licença: produto [comercial](https://tablum.io/pricing) com período gratuito de 3 meses.

Experimente gratuitamente [na nuvem](https://tablum.io/try).
Saiba mais sobre o produto em [TABLUM.IO](https://tablum.io/)

<div id="ckman">
  ### CKMAN
</div>

[CKMAN](https://www.github.com/housepower/ckman) é uma ferramenta para gerenciar e monitorar clusters do ClickHouse!

Recursos:

* Implantação automatizada rápida e prática de clusters por meio de uma interface no navegador
* Os clusters podem ser ampliados ou reduzidos
* Balanceamento de carga dos dados do cluster
* Atualização online do cluster
* Modificação da configuração do cluster pela página
* Fornece monitoramento dos nós do cluster e do ZooKeeper
* Monitora o status de tabelas e partições, além de instruções SQL lentas
* Fornece uma página de execução de SQL fácil de usar

<div id="1bench">
  ### 1bench
</div>

[1bench](https://1bench.dev) é uma GUI desktop nativa para vários bancos de dados, com suporte de primeira classe ao ClickHouse — abrangendo visão geral do servidor, gerenciamento de esquema, busca vetorial e navegação em grandes conjuntos de resultados.

Recursos:

* Visão geral do servidor ao se conectar — versão, uptime, consultas em execução, merges ativos, partes e tamanhos de armazenamento, status da réplica, clusters e nós de relance.
* Construtor visual de consultas (seletores de colunas, filtros, ordenação, limite) ao lado de um editor SQL Monaco com realce de sintaxe e histórico de consultas por conexão.
* Assistente visual de `CREATE TABLE` com suporte a variantes de `MergeTree`, `ORDER BY`, `PARTITION BY`, `SETTINGS` e encapsulamento automático com `Nullable()`.
* Suporte nativo a tipos do ClickHouse — `Nullable`, `Array`, `LowCardinality`, objetos aninhados.
* Suporte a busca vetorial — colunas de embedding `Array(Float32)` renderizadas como células vetoriais compactas, visualização 2D de embeddings e Find Similar via `cosineDistance`.
* Edição inline de dados em tabelas de resultados com salvamento em lote, além de exportação e importação em CSV/JSON/SQL usando os formatos nativos do ClickHouse.
* Opções de conexão: HTTP/HTTPS, túnel SSH para clusters privados atrás de um firewall e modo somente leitura opcional para navegação segura em produção.
* Funciona com ClickHouse Cloud e ambientes auto-hospedado.