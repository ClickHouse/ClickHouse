export const LogoCard = ({ title, img, imgAlt, href, external, children }) => {
  const [dark, setDark] = useState(false);
  useEffect(() => {
    const check = () => setDark(document.documentElement.classList.contains('dark'));
    check();
    const obs = new MutationObserver(check);
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
    return () => obs.disconnect();
  }, []);
  const assetBase = typeof window !== 'undefined' && window.location.pathname.startsWith('/docs') ? '/docs' : '';
  const withBase = (p) => p && p.startsWith('/') ? assetBase + p : p;
  return <a href={href} className="cs-card-link" target={external ? '_blank' : undefined} rel={external ? 'noreferrer' : undefined} style={{ textDecoration: 'none', textDecorationLine: 'none', color: 'inherit', display: 'block', height: '100%' }}>
    <div
      className="cs-card"
      style={{
        display: 'flex', alignItems: 'center', gap: '20px', height: '100%', padding: '20px',
        borderRadius: '12px',
        border: dark ? '1px solid rgba(255,255,255,0.08)' : '1px solid #e0e0e0',
        backgroundColor: dark ? 'rgba(65,65,65,0.5)' : '#ffffff',
        boxShadow: dark ? '0 1px 3px rgba(0,0,0,0.2)' : '0 1px 3px rgba(0,0,0,0.1)',
        cursor: 'pointer',
      }}
    >
      <div style={{ width: 80, height: 80, flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', borderRadius: '16px', backgroundColor: dark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.04)' }}>
        <img src={withBase(img)} alt={imgAlt || ''} width="52" height="52" style={{ width: 52, height: 52, objectFit: 'contain', margin: 0, borderRadius: 0, boxShadow: 'none' }} />
      </div>
      <div style={{ minWidth: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '16px', fontWeight: 600, lineHeight: 1.3, color: dark ? '#ffffff' : '#111111' }}>
          <span>{title}</span>
          {external && <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" style={{ opacity: 0.6, flexShrink: 0 }}><path d="M4.5 11.5l7-7M6 4.5h5.5V10" /></svg>}
        </div>
        <div style={{ marginTop: '6px', fontSize: '14px', lineHeight: 1.5, color: dark ? '#b4b4b4' : '#555555' }}>{children}</div>
      </div>
    </div>
  </a>;
};

