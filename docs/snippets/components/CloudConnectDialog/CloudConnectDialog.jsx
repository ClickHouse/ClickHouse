export const CloudConnectDialog = () => (
  <figure className="cloud-connect-poc" aria-label="Example ClickHouse Cloud connection dialog">
    <div className="cloud-connect-poc__dialog">
      <header className="cloud-connect-poc__header">
        <div>
          <h4>Connect to example-service</h4>
          <p>Use these credentials to connect to your ClickHouse Cloud service.</p>
        </div>
        <span className="cloud-connect-poc__close" aria-hidden="true">×</span>
      </header>

      <section className="cloud-connect-poc__credentials" aria-label="Example credentials">
        <div className="cloud-connect-poc__credential">
          <span className="cloud-connect-poc__label">Username</span>
          <span>default</span>
          <span className="cloud-connect-poc__icon"><svg aria-hidden="true" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.7"><rect x="8" y="8" width="11" height="11" rx="2" /><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2" /></svg></span>
        </div>
        <div className="cloud-connect-poc__credential">
          <span className="cloud-connect-poc__label">Password</span>
          <span aria-label="Password hidden">••••••••••••</span>
          <span className="cloud-connect-poc__actions"><svg aria-hidden="true" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.7"><path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z" /><circle cx="12" cy="12" r="2.5" /></svg><svg aria-hidden="true" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.7"><rect x="8" y="8" width="11" height="11" rx="2" /><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2" /></svg></span>
        </div>
        <span className="cloud-connect-poc__download"><svg aria-hidden="true" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.7"><path d="M12 3v12m0 0 4-4m-4 4-4-4M4 16v4h16v-4" /></svg></span>
      </section>

      <div className="cloud-connect-poc__protocol">
        <span>Connect with:</span>
        <span className="cloud-connect-poc__select"><strong>HTTP</strong> HTTPS <span aria-hidden="true">⌄</span></span>
      </div>

      <p className="cloud-connect-poc__instruction">Run the following command from your terminal:</p>
      <pre className="cloud-connect-poc__code" aria-label="Example curl command"><code><span>1</span> curl --user <b>'default:&lt;password&gt;'</b> \{`\n`}<span>2</span>   --data-binary <b>'SELECT 1'</b> \{`\n`}<span>3</span>   https://example.us-west-2.aws.clickhouse.cloud:8443</code><svg aria-hidden="true" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.7"><rect x="8" y="8" width="11" height="11" rx="2" /><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2" /></svg></pre>

      <p className="cloud-connect-poc__help">See the <span>HTTP interface documentation ↗</span> for more information.</p>
    </div>
    <figcaption>Example connection details. Values are placeholders and controls are not interactive.</figcaption>

    <style>{`
      .cloud-connect-poc {
        --cc-bg: #ffffff;
        --cc-panel: #f6f7fa;
        --cc-code: #f4f5f8;
        --cc-border: #e2e4e8;
        --cc-ink: #191b1f;
        --cc-muted: #666b75;
        --cc-accent: #477cf4;
        width: min(100%, 760px);
        margin: 1.5rem auto;
        color: var(--cc-ink);
        font-family: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }
      .dark .cloud-connect-poc {
        --cc-bg: #202124;
        --cc-panel: #282a2e;
        --cc-code: #282a2e;
        --cc-border: #3b3e43;
        --cc-ink: #f4f5f6;
        --cc-muted: #aeb2ba;
        --cc-accent: #fdff75;
      }
      .cloud-connect-poc__dialog {
        padding: clamp(1rem, 4vw, 2.5rem);
        border: 1px solid var(--cc-border);
        border-radius: 18px;
        background: var(--cc-bg);
        box-shadow: 0 16px 44px rgb(0 0 0 / 16%);
      }
      .cloud-connect-poc__header { display: flex; justify-content: space-between; gap: 1rem; margin-bottom: 1.5rem; }
      .cloud-connect-poc__header h4 { margin: 0 0 .35rem; color: var(--cc-ink); font-size: clamp(1.25rem, 3vw, 1.75rem); line-height: 1.25; }
      .cloud-connect-poc__header p, .cloud-connect-poc__help { margin: 0; color: var(--cc-muted); font-size: .95rem; }
      .cloud-connect-poc__close { color: var(--cc-ink); font-size: 1.8rem; line-height: 1; }
      .cloud-connect-poc__credentials { position: relative; display: grid; gap: 1rem; padding: 1.25rem 4.5rem 1.25rem 1.25rem; border-radius: 8px; background: var(--cc-panel); }
      .cloud-connect-poc__credential { display: grid; grid-template-columns: 1fr auto; gap: .35rem 1rem; align-items: center; }
      .cloud-connect-poc__credential + .cloud-connect-poc__credential { padding-top: 1rem; border-top: 1px solid var(--cc-border); }
      .cloud-connect-poc__label { grid-column: 1 / -1; color: var(--cc-muted); font-size: .82rem; font-weight: 600; }
      .cloud-connect-poc__icon, .cloud-connect-poc__actions { display: flex; gap: .8rem; color: var(--cc-muted); }
      .cloud-connect-poc__download { position: absolute; display: grid; place-items: center; right: 0; top: 1rem; bottom: 1rem; width: 3.5rem; border-left: 1px solid var(--cc-border); }
      .cloud-connect-poc__protocol { display: flex; align-items: center; gap: 1rem; margin: 1.5rem 0; }
      .cloud-connect-poc__select { display: flex; align-items: center; gap: .6rem; min-width: 210px; padding: .65rem .8rem; border: 1px solid var(--cc-border); border-radius: 6px; }
      .cloud-connect-poc__select strong { padding: .1rem .25rem; border: 1px solid currentColor; border-radius: 3px; font-size: .65rem; }
      .cloud-connect-poc__select span { margin-left: auto; }
      .cloud-connect-poc__instruction { margin: 0 0 .65rem; }
      .cloud-connect-poc__code { position: relative; margin: 0; padding: 1rem 3rem 1rem 1rem; overflow-x: auto; border-radius: 8px; background: var(--cc-code); color: var(--cc-ink); font-size: clamp(.72rem, 2vw, .9rem); line-height: 1.8; white-space: pre; }
      .cloud-connect-poc__code > svg { position: absolute; top: 1rem; right: 1rem; color: var(--cc-muted); }
      .cloud-connect-poc__code code > span { display: inline-block; width: 1.5rem; color: var(--cc-muted); user-select: none; }
      .cloud-connect-poc__code b { color: #4f8500; font-weight: 500; }
      .dark .cloud-connect-poc__code b { color: #b6d979; }
      .cloud-connect-poc__help { margin-top: 1.25rem; }
      .cloud-connect-poc__help span { color: var(--cc-accent); }
      .cloud-connect-poc figcaption { margin-top: .65rem; color: var(--cc-muted); font-size: .78rem; text-align: center; }
      @media (max-width: 560px) {
        .cloud-connect-poc__credentials { padding-right: 3.5rem; }
        .cloud-connect-poc__protocol { align-items: flex-start; flex-direction: column; gap: .5rem; }
        .cloud-connect-poc__select { width: 100%; }
      }
    `}</style>
  </figure>
)
