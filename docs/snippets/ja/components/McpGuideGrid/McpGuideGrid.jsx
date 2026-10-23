const localizeHref = (href) => {
  if (/^\/(ar|es|fr|ja|ko|pt-BR|ru|zh)(?:\/|$)/.test(href)) return href;
  if (typeof window === 'undefined') return href;
  const match = window.location.pathname.match(/^\/(?:docs\/)?(ar|es|fr|ja|ko|pt-BR|ru|zh)(?:\/|$)/);
  return match ? `/${match[1]}${href}` : href;
};

export const McpGuideGrid = ({ variant = "guides" }) => {
  const clients = [
    { name: "Claude Code", logo: "/images/logo-claudecode-color.svg" },
    { name: "Claude Desktop", logo: "/images/logo-claude.svg" },
    { name: "ChatGPT", logo: "/images/logo-codex.svg" },
    { name: "Cursor", logo: "/images/logo-cursor.webp" },
    { name: "Windsurf", logo: "/images/logo-windsurf.svg" }
  ]

  const guides = [
    { name: "Streamlit", logo: "/images/mcp-integrations/streamlit.svg", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/streamlit") },
    { name: "LangChain and LangGraph", logo: "/images/integrations/logos/langchain.svg", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/langchain"), lightInvert: true },
    { name: "LlamaIndex", logo: "/images/mcp-integrations/llamaindex.png", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/llamaindex") },
    { name: "PydanticAI", logo: "/images/mcp-integrations/pydantic-ai.png", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/pydantic-ai") },
    { name: "SlackBot", logo: "/images/mcp-integrations/slack.png", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/slackbot") },
    { name: "Agno", logo: "/images/mcp-integrations/agno.png", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/agno") },
    { name: "Chainlit", logo: "/images/mcp-integrations/chainlit.svg", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/chainlit") },
    { name: "Claude Agent SDK", logo: "/images/logo-claude.svg", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/claude-agent-sdk") },
    { name: "CopilotKit", logo: "/images/mcp-integrations/copilotkit.svg", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/copilotkit") },
    { name: "CrewAI", logo: "/images/mcp-integrations/crewai.png", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/crewai") },
    { name: "DSPy", logo: "/images/mcp-integrations/dspy.svg", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/dspy") },
    { name: "mcp-agent", logo: "/images/mcp-integrations/mcp-agent.png", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/mcp-agent") },
    {
      name: "Microsoft Agent Framework",
      logo: "/images/mcp-integrations/microsoft-agent-framework.png",
      href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/microsoft-agent-framework")
    },
    { name: "Upsonic", logo: "/images/mcp-integrations/upsonic.jpg", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/upsonic") },
    { name: "OpenAI Agents SDK", logo: "/images/logo-codex.svg", href: localizeHref("/guides/use-cases/ai-ml/MCP/ai-agent-libraries/openai-agents") },
    { name: "Ollama", logo: "/images/mcp-integrations/ollama.svg", href: localizeHref("/guides/use-cases/ai-ml/MCP/ollama") },
    { name: "AnythingLLM", logo: "/images/mcp-integrations/anythingllm.png", href: localizeHref("/guides/use-cases/ai-ml/MCP/anythingllm") },
    { name: "Jan.ai", logo: "/images/mcp-integrations/jan.svg", href: localizeHref("/guides/use-cases/ai-ml/MCP/janai") },
    { name: "LibreChat", logo: "/images/mcp-integrations/librechat.svg", href: localizeHref("/guides/use-cases/ai-ml/MCP/librechat") },
    { name: "Open WebUI", logo: "/images/mcp-integrations/open-webui.svg", href: localizeHref("/guides/use-cases/ai-ml/MCP/open-webui") }
  ]

  if (variant === "clients") {
    return (
      <div className="card not-prose contents">
        <a
          href={localizeHref("/guides/use-cases/ai-ml/MCP/claude-desktop")}
          className="block no-underline focus:no-underline focus-visible:no-underline"
          style={{ textDecoration: "none", outline: "none" }}
        >
          <div className="flex min-h-[112px] flex-col items-center gap-5 rounded-xl border border-gray-200 bg-white p-5 shadow-sm dark:border-white/10 dark:bg-[#282828] md:flex-row md:justify-between">
            <div className="text-center md:text-left">
              <div className="text-base font-semibold text-black dark:text-white">ClickHouse MCPサーバーをセットアップする</div>
              <div className="mt-1 text-sm text-gray-600 dark:text-gray-400">Claude Code、Claude Desktop、Codex、ChatGPT、Cursor、Windsurf</div>
            </div>

            <div className="flex flex-wrap items-center justify-center gap-2">
              {clients.map((client) => (
                <img key={client.name} src={client.logo} alt={`${client.name} logo`} title={client.name} className="h-8 w-8 object-contain" style={{ pointerEvents: "none" }} />
              ))}
            </div>
          </div>
        </a>
      </div>
    )
  }

  return (
    <div className="my-8 grid grid-cols-2 gap-4 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 2xl:grid-cols-6">
      {guides.map((guide) => (
        <div key={guide.href} className="card not-prose contents">
          <a href={guide.href} className="block no-underline focus:no-underline focus-visible:no-underline" style={{ textDecoration: "none", outline: "none" }}>
            <div className="flex aspect-square min-h-[120px] flex-col items-center justify-center rounded-xl border border-gray-200 bg-white p-4 text-center shadow-sm dark:border-white/10 dark:bg-[#282828]">
              {guide.lightInvert ? (
                <img src={guide.logo} alt={`${guide.name} logo`} className="ch-logo-invert h-16 w-16 object-contain" style={{ pointerEvents: "none" }} />
              ) : (
                <img src={guide.logo} alt={`${guide.name} logo`} className="h-16 w-16 object-contain" style={{ pointerEvents: "none" }} />
              )}
              <div className="mt-3 text-sm font-semibold text-black dark:text-white">{guide.name}</div>
            </div>
          </a>
        </div>
      ))}
    </div>
  )
}