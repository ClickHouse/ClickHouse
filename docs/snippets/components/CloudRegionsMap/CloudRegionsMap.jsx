// Port of ClickHouse/marketing-website PresenceBubbleMap and ScaleToContainer.
// Region availability follows the tables on the containing documentation page.
export const CloudRegionsMap = () => {
  // Images rendered from JavaScript do not receive Mintlify's asset URL rewriting.
  const assetBase = typeof window !== 'undefined' && /^\/docs(?:\/|$)/.test(window.location.pathname)
    ? '/docs'
    : '';
  const regions = [
  {
    "code": "af-south-1",
    "name": "Africa (Cape Town)",
    "provider": "AWS",
    "tags": [
      "Private",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 53.5,
    "y": 84.0,
    "position": "middle-right",
    "hoverOnly": false
  },
  {
    "code": "ap-east-1",
    "name": "Asia Pacific (Hong Kong)",
    "provider": "AWS",
    "tags": [
      "Private",
      "BYOC"
    ],
    "x": 79,
    "y": 51,
    "position": "top-left",
    "hoverOnly": false
  },
  {
    "code": "ap-east-2",
    "name": "Asia Pacific (Taipei)",
    "provider": "AWS",
    "tags": [
      "Private"
    ],
    "x": 82,
    "y": 49,
    "position": "bottom-right",
    "hoverOnly": false
  },
  {
    "code": "ap-northeast-1",
    "name": "Asia Pacific (Tokyo)",
    "provider": "AWS",
    "tags": [
      "Public",
      "BYOC"
    ],
    "x": 86.5,
    "y": 43.0,
    "position": "top-right",
    "hoverOnly": false,
    "labelOffsetY": -5
  },
  {
    "code": "ap-northeast-2",
    "name": "Asia Pacific (Seoul)",
    "provider": "AWS",
    "tags": [
      "Public",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 83.0,
    "y": 42.0,
    "position": "middle-left",
    "hoverOnly": false
  },
  {
    "code": "ap-south-1",
    "name": "Asia Pacific (Mumbai)",
    "provider": "AWS",
    "tags": [
      "Public",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 68.0,
    "y": 53.0,
    "position": "middle-right",
    "hoverOnly": false
  },
  {
    "code": "ap-southeast-1",
    "name": "Asia Pacific (Singapore)",
    "provider": "AWS",
    "tags": [
      "Public",
      "BYOC"
    ],
    "x": 76.0,
    "y": 63.0,
    "position": "top-right",
    "hoverOnly": false
  },
  {
    "code": "ap-southeast-2",
    "name": "Asia Pacific (Sydney)",
    "provider": "AWS",
    "tags": [
      "Public",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 90.0,
    "y": 84.0,
    "position": "bottom-right",
    "hoverOnly": false
  },
  {
    "code": "ap-southeast-3",
    "name": "Asia Pacific (Jakarta)",
    "provider": "AWS",
    "tags": [
      "Private"
    ],
    "x": 78,
    "y": 69,
    "position": "bottom-right",
    "hoverOnly": false
  },
  {
    "code": "ap-southeast-5",
    "name": "Asia Pacific (Malaysia)",
    "provider": "AWS",
    "tags": [
      "Private"
    ],
    "x": 75,
    "y": 61,
    "position": "top-left",
    "hoverOnly": false
  },
  {
    "code": "ca-central-1",
    "name": "Canada (Central)",
    "provider": "AWS",
    "tags": [
      "Public",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 28.1,
    "y": 36.3,
    "position": "top-center",
    "hoverOnly": false,
    "labelOffsetY": -18
  },
  {
    "code": "eu-central-1",
    "name": "Europe (Frankfurt)",
    "provider": "AWS",
    "tags": [
      "Public",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 50.5,
    "y": 35.5,
    "position": "middle-right",
    "hoverOnly": false
  },
  {
    "code": "eu-north-1",
    "name": "Europe (Stockholm)",
    "provider": "AWS",
    "tags": [
      "Private",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 53.0,
    "y": 25.5,
    "position": "middle-right",
    "hoverOnly": false
  },
  {
    "code": "eu-west-1",
    "name": "Europe (Ireland)",
    "provider": "AWS",
    "tags": [
      "Public",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 46.0,
    "y": 30.0,
    "position": "top-left",
    "hoverOnly": false
  },
  {
    "code": "eu-west-2",
    "name": "Europe (London)",
    "provider": "AWS",
    "tags": [
      "Public",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 48.0,
    "y": 32.0,
    "position": "bottom-left",
    "hoverOnly": false
  },
  {
    "code": "il-central-1",
    "name": "Israel (Tel Aviv)",
    "provider": "AWS",
    "tags": [
      "Public",
      "BYOC"
    ],
    "x": 58,
    "y": 44,
    "position": "middle-left",
    "hoverOnly": false
  },
  {
    "code": "mx-central-1",
    "name": "Mexico (Central)",
    "provider": "AWS",
    "tags": [
      "Private",
      "HIPAA",
      "PCI"
    ],
    "x": 21,
    "y": 55,
    "position": "middle-right",
    "hoverOnly": false
  },
  {
    "code": "sa-east-1",
    "name": "South America (São Paulo)",
    "provider": "AWS",
    "tags": [
      "Private",
      "HIPAA",
      "PCI"
    ],
    "x": 36,
    "y": 80,
    "position": "middle-right",
    "hoverOnly": false
  },
  {
    "code": "us-east-1",
    "name": "US East (N. Virginia)",
    "provider": "AWS",
    "tags": [
      "Public",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 27.0,
    "y": 43.0,
    "position": "top-right",
    "hoverOnly": false
  },
  {
    "code": "us-east-2",
    "name": "US East (Ohio)",
    "provider": "AWS",
    "tags": [
      "Public",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 25.5,
    "y": 38.5,
    "position": "top-right",
    "hoverOnly": false
  },
  {
    "code": "us-west-2",
    "name": "US West (Oregon)",
    "provider": "AWS",
    "tags": [
      "Public",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 15.25,
    "y": 37.0,
    "position": "middle-left",
    "hoverOnly": false
  },
  {
    "code": "asia-northeast1",
    "name": "Tokyo, Japan",
    "provider": "Google Cloud",
    "tags": [
      "Public",
      "BYOC"
    ],
    "x": 85.5,
    "y": 45.0,
    "position": "bottom-right",
    "hoverOnly": false
  },
  {
    "code": "asia-southeast1",
    "name": "Jurong West, Singapore",
    "provider": "Google Cloud",
    "tags": [
      "Public",
      "BYOC"
    ],
    "x": 76.5,
    "y": 65.0,
    "position": "bottom-right",
    "hoverOnly": false
  },
  {
    "code": "australia-southeast1",
    "name": "Sydney, Australia",
    "provider": "Google Cloud",
    "tags": [
      "Private"
    ],
    "x": 89.0,
    "y": 85.0,
    "position": "bottom-left",
    "hoverOnly": false
  },
  {
    "code": "europe-west2",
    "name": "London, United Kingdom",
    "provider": "Google Cloud",
    "tags": [
      "Public"
    ],
    "x": 47,
    "y": 33,
    "position": "middle-left",
    "hoverOnly": true
  },
  {
    "code": "europe-west3",
    "name": "Frankfurt, Germany",
    "provider": "Google Cloud",
    "tags": [
      "Private"
    ],
    "x": 52.5,
    "y": 34.5,
    "position": "middle-right",
    "hoverOnly": true,
    "labelOffsetY": 5
  },
  {
    "code": "europe-west4",
    "name": "Eemshaven, Netherlands",
    "provider": "Google Cloud",
    "tags": [
      "Public",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 49.5,
    "y": 34.5,
    "position": "top-right",
    "hoverOnly": false
  },
  {
    "code": "europe-west6",
    "name": "Zurich, Switzerland",
    "provider": "Google Cloud",
    "tags": [
      "Private"
    ],
    "x": 51.5,
    "y": 36.5,
    "position": "bottom-right",
    "hoverOnly": true
  },
  {
    "code": "northamerica-northeast1",
    "name": "Montréal, Québec",
    "provider": "Google Cloud",
    "tags": [
      "Private"
    ],
    "x": 28.9,
    "y": 37.3,
    "position": "middle-right",
    "hoverOnly": true
  },
  {
    "code": "us-central1",
    "name": "Council Bluffs, Iowa",
    "provider": "Google Cloud",
    "tags": [
      "Private",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 21.0,
    "y": 37.0,
    "position": "top-center",
    "hoverOnly": false
  },
  {
    "code": "us-east1",
    "name": "Moncks Corner, South Carolina",
    "provider": "Google Cloud",
    "tags": [
      "Public",
      "HIPAA",
      "PCI",
      "BYOC"
    ],
    "x": 27.5,
    "y": 45.0,
    "position": "middle-right",
    "hoverOnly": false
  },
  {
    "code": "us-west1",
    "name": "The Dalles, Oregon",
    "provider": "Google Cloud",
    "tags": [
      "Private"
    ],
    "x": 15.0,
    "y": 39.0,
    "position": "bottom-left",
    "hoverOnly": false
  },
  {
    "code": "australiaeast",
    "name": "Australia East",
    "provider": "Azure",
    "tags": [
      "Private",
      "BYOC"
    ],
    "x": 91,
    "y": 83,
    "position": "top-right",
    "hoverOnly": false
  },
  {
    "code": "eastus2",
    "name": "East US 2",
    "provider": "Azure",
    "tags": [
      "Public",
      "BYOC"
    ],
    "x": 27.6,
    "y": 43.5,
    "position": "bottom-right",
    "hoverOnly": true
  },
  {
    "code": "germanywestcentral",
    "name": "Germany West Central",
    "provider": "Azure",
    "tags": [
      "Public",
      "BYOC"
    ],
    "x": 52.0,
    "y": 36.0,
    "position": "bottom-right",
    "hoverOnly": false
  },
  {
    "code": "japaneast",
    "name": "Japan East",
    "provider": "Azure",
    "tags": [
      "Private",
      "BYOC"
    ],
    "x": 86.5,
    "y": 45.5,
    "position": "top-right",
    "hoverOnly": false,
    "labelOffsetY": 5
  },
  {
    "code": "uaenorth",
    "name": "UAE North",
    "provider": "Azure",
    "tags": [
      "Private"
    ],
    "x": 63,
    "y": 50,
    "position": "bottom-left",
    "hoverOnly": false
  },
  {
    "code": "westus3",
    "name": "West US 3",
    "provider": "Azure",
    "tags": [
      "Public",
      "BYOC"
    ],
    "x": 19,
    "y": 44,
    "position": "bottom-center",
    "hoverOnly": false
  }
];
  const [activeFilter, setActiveFilter] = useState(null);
  const [regionSearch, setRegionSearch] = useState('');
  const [regionOpen, setRegionOpen] = useState(false);
  const [optionIndex, setOptionIndex] = useState(-1);
  const regionInput = useRef(null);
  const regionList = useRef(null);
  useEffect(() => {
    if (optionIndex >= 0) regionList.current?.children[optionIndex]?.scrollIntoView({block: 'nearest'});
  }, [optionIndex]);
  const [hovered, setHovered] = useState(null);
  const [focused, setFocused] = useState(null);
  const [scale, setScale] = useState(1);
  const container = useRef(null);
  useEffect(() => {
    const element = container.current;
    const resize = () => setScale(element.clientWidth / 1139);
    resize();
    const observer = new ResizeObserver(resize);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  const colors = { AWS: 'var(--map-aws)', 'Google Cloud': 'var(--map-gcp)', Azure: 'var(--map-azure)' };
  const filters = ['AWS', 'GCP', 'Azure', 'Private regions', 'HIPAA compliant'];
  const matches = (region, filter) => !filter || (filter === 'GCP' ? region.provider === 'Google Cloud' : filter === 'Private regions' ? region.tags.includes('Private') : filter === 'HIPAA compliant' ? region.tags.includes('HIPAA') : region.provider === filter);
  const positions = {
    'top-left': {bottom: '100%', right: '100%'},
    'top-center': {bottom: '100%', left: '50%', transform: 'translateX(-50%)'},
    'top-right': {bottom: '100%', left: '100%'},
    'middle-left': {top: '50%', right: '100%', transform: 'translateY(-50%)'},
    'middle-right': {top: '50%', left: '100%', transform: 'translateY(-50%)'},
    'bottom-left': {top: '100%', right: '100%'},
    'bottom-center': {top: '100%', left: '50%', transform: 'translateX(-50%)'},
    'bottom-right': {top: '100%', left: '100%'}
  };
  const emphasized = hovered || focused;
  const query = regionSearch.trim().toLowerCase();
  const matchesSearch = region => `${region.provider} ${region.code} ${region.name}`.toLowerCase().includes(query);
  const options = regions.filter(region => matchesSearch(region) && matches(region, activeFilter));
  const regionCount = options.length;
  const chooseRegion = region => {
    setRegionSearch(region.code);
    setRegionOpen(false);
    setOptionIndex(-1);
    regionInput.current?.focus();
  };
  return (
    <section aria-label="Cloud region map" className="cloud-presence-map" style={{background: 'var(--map-bg)', color: 'var(--map-text)', borderRadius: 12, padding: '24px 16px', margin: '24px 0', overflow: 'visible'}}>
      <style>{`
        .cloud-presence-map { --map-bg: #161616; --map-text: #eee; --map-muted: #a3a3a3; --map-surface: #242424; --map-border: #454545; --map-hover: #353535; --map-focus: #fdff75; --map-aws: #FBFF46; --map-gcp: #FF7575; --map-azure: #00CBEB; }

        .cloud-presence-map .region-search { width: 100%; box-sizing: border-box; background: var(--map-surface); color: var(--map-text); border: 1px solid var(--map-border); border-radius: 4px; padding: 8px 38px 8px 36px; font: inherit; font-size: 14px; }
        .cloud-presence-map .region-search::placeholder { color: var(--map-muted); }
        .cloud-presence-map .region-search:focus { border-color: var(--map-focus); outline: 2px solid var(--map-focus); outline-offset: 2px; }

        .cloud-presence-map .presence-pin,
        .cloud-presence-map .presence-label,
        .cloud-presence-map .presence-filter { transition: opacity 150ms ease; transition-duration: 150ms !important; }
        .cloud-presence-map .presence-filter { background: transparent; }
        .cloud-presence-map .presence-filter:hover { background: var(--map-hover); }
        .cloud-presence-map button:focus-visible { outline: 2px solid var(--map-focus); outline-offset: 4px; }
        @media (prefers-reduced-motion: reduce) {
          .cloud-presence-map .presence-pin,
          .cloud-presence-map .presence-label,
          .cloud-presence-map .presence-filter { transition-duration: 0s !important; }
        }
      `}</style>
      <div ref={container} style={{position: 'relative', height: 543 * scale}}>
        <div style={{position: 'relative', width: 1139, height: 543, transform: `scale(${scale})`, transformOrigin: 'top left'}}>
          <img src={`${assetBase}/images/cloud/regions-map.png`} width="1139" height="543" alt="World map of supported cloud regions" style={{width: 1139, maxWidth: 'none', height: 543, margin: 0, pointerEvents: 'none'}} />
          {regions.map(region => {
            const isActive = matchesSearch(region) && matches(region, activeFilter);
            const color = colors[region.provider];
            const isPrivate = region.tags.includes('Private');
            const isEmphasized = emphasized === region.code;
            const showLabel = !region.hoverOnly || isEmphasized || !!activeFilter || !!query;
            const corner = /^(top|bottom)-(left|right)$/.test(region.position);
            return <button type="button" className="presence-pin" key={region.code} aria-label={`${region.provider}: ${region.name} (${region.code}) — ${region.tags.join(', ')}`} aria-hidden={!isActive} tabIndex={isActive ? 0 : -1} onMouseEnter={() => setHovered(region.code)} onMouseLeave={() => setHovered(null)} onFocus={() => setFocused(region.code)} onBlur={() => setFocused(null)} style={{position: 'absolute', left: `${region.x}%`, top: `${region.y}%`, width: 10, height: 10, padding: 0, border: 0, background: 'transparent', transform: 'translate(-50%, -50%)', color, cursor: 'default', opacity: !isActive ? 0 : emphasized && !isEmphasized ? .4 : 1, pointerEvents: isActive ? 'auto' : 'none', zIndex: isEmphasized ? 20 : 1}}>
              <span style={{position: 'relative', display: 'block', width: 10, height: 10, boxSizing: 'border-box', borderRadius: '50%', border: isPrivate ? `2px solid ${color}` : 'none', background: isPrivate ? 'transparent' : color}}>
                <span style={{position: 'absolute', inset: 0, borderRadius: '50%', transform: 'scale(2.5)', opacity: .1, background: color, pointerEvents: 'none'}} />
                <span style={{position: 'absolute', inset: 0, borderRadius: '50%', transform: 'scale(4)', opacity: .1, background: color, pointerEvents: 'none'}} />
              </span>
              <span className="presence-label" style={{position: 'absolute', ...positions[region.position], translate: `0 ${region.labelOffsetY || 0}px`, padding: corner ? 5.6 : 9, whiteSpace: 'nowrap', fontSize: 17, fontWeight: 500, lineHeight: 1, opacity: showLabel ? 1 : 0, pointerEvents: showLabel ? 'auto' : 'none', textShadow: '-1px -1px 0 var(--map-bg), 1px -1px 0 var(--map-bg), -1px 1px 0 var(--map-bg), 1px 1px 0 var(--map-bg)'}}>{region.code}</span>
            </button>;
          })}
        </div>
      </div>
      <div role="group" aria-label="Filter cloud regions" style={{position: 'relative', zIndex: 30, display: 'flex', flexWrap: 'wrap', justifyContent: 'center', columnGap: 32, rowGap: 16, marginTop: 48}}>
      <div style={{display: 'flex', flexWrap: 'wrap', alignItems: 'center', justifyContent: 'flex-start', gap: 16, width: '100%'}}>
        <div onBlur={event => {if (!event.currentTarget.contains(event.relatedTarget)) setRegionOpen(false);}} style={{position: 'relative', width: 360, maxWidth: '100%'}}>
          <svg aria-hidden="true" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" style={{position: 'absolute', left: 12, top: '50%', transform: 'translateY(-50%)', color: 'var(--map-muted)', pointerEvents: 'none'}}><circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 4.5 4.5"/></svg>
          <input ref={regionInput} className="region-search" type="text" role="combobox" aria-label="Search regions" aria-expanded={regionOpen} aria-controls="cloud-region-options" aria-autocomplete="list" aria-activedescendant={regionOpen && optionIndex >= 0 ? `cloud-region-option-${optionIndex}` : undefined} autoComplete="off" placeholder="Search or select a region…" value={regionSearch} onClick={() => setRegionOpen(true)} onChange={event => {setRegionSearch(event.target.value); setRegionOpen(true); setOptionIndex(-1); setHovered(null); setFocused(null);}} onKeyDown={event => {
            if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
              event.preventDefault();
              setRegionOpen(true);
              setOptionIndex(old => options.length ? event.key === 'ArrowDown' ? Math.min(old + 1, options.length - 1) : old < 0 ? options.length - 1 : Math.max(old - 1, 0) : -1);
            } else if (event.key === 'Enter' && regionOpen && options[optionIndex]) {
              event.preventDefault(); chooseRegion(options[optionIndex]);
            } else if (event.key === 'Escape') {setRegionOpen(false); setOptionIndex(-1);}
          }} />
          <button type="button" aria-label={regionOpen ? 'Close region list' : 'Browse regions'} aria-expanded={regionOpen} aria-controls="cloud-region-options" onClick={() => {setRegionOpen(old => !old); setOptionIndex(-1); regionInput.current?.focus();}} style={{position: 'absolute', right: 4, top: 4, bottom: 4, width: 30, display: 'grid', placeItems: 'center', background: 'transparent', border: 0, color: 'var(--map-muted)', cursor: 'pointer'}}><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d={regionOpen ? 'm6 15 6-6 6 6' : 'm6 9 6 6 6-6'}/></svg></button>
          {regionOpen && <div style={{position: 'absolute', top: 'calc(100% + 8px)', left: 0, width: '100%', zIndex: 40, background: 'var(--map-surface)', border: '1px solid var(--map-border)', borderRadius: 4, boxShadow: '0 8px 24px #0006', overflow: 'hidden'}}>
            <div ref={regionList} id="cloud-region-options" role="listbox" aria-label="Regions" style={{maxHeight: 240, overflowY: 'auto'}}>
              {options.map((region, index) => <div key={region.code} id={`cloud-region-option-${index}`} role="option" aria-selected={regionSearch === region.code} onMouseDown={event => event.preventDefault()} onMouseEnter={() => setOptionIndex(index)} onClick={() => chooseRegion(region)} style={{padding: '10px 12px', cursor: 'pointer', background: index === optionIndex ? 'var(--map-hover)' : 'transparent', borderBottom: '1px solid var(--map-border)', fontSize: 13, lineHeight: 1.5}}>
                <div style={{color: 'var(--map-text)'}}>{region.name}</div>
                <div style={{color: 'var(--map-muted)'}}><span style={{color: colors[region.provider]}}>{region.provider}</span> · {region.code}</div>
              </div>)}
            </div>
            {options.length === 0 && <div style={{padding: 12, color: 'var(--map-muted)', fontSize: 13}}>No matching regions</div>}
          </div>}
        </div>
        <span role="status" style={{fontSize: 12, color: 'var(--map-muted)'}}>{regionCount} matching {regionCount === 1 ? 'region' : 'regions'}</span>
      </div>
      {regionCount === 0 && <p style={{fontSize: 14, color: 'var(--map-muted)'}}>No regions match these filters. Try another region code or location, or clear the filters.</p>}
        {filters.map(filter => {
          const filterColors = [...new Set(regions.filter(region => matches(region, filter)).map(region => colors[region.provider]))];
          return <button type="button" className="presence-filter" key={filter} aria-pressed={activeFilter === filter} onClick={() => {setActiveFilter(old => old === filter ? null : filter); setOptionIndex(-1); setRegionOpen(false); setHovered(null); setFocused(null);}} style={{display: 'flex', alignItems: 'center', gap: 16, border: 0, borderRadius: 6, padding: '4px 12px', color: 'var(--map-text)', fontSize: 14, cursor: 'pointer', opacity: !activeFilter || activeFilter === filter ? 1 : .5}}>
            <span style={{display: 'inline-flex', gap: 8}}>
              {filter === 'HIPAA compliant' ? <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true"><path d="M12 22s8-4 8-11V5l-8-3-8 3v6c0 7 8 11 8 11Z"/><path d="m9 12 2 2 4-4"/></svg> : filterColors.map(color => <span key={color} style={{width: 10, height: 10, boxSizing: 'border-box', borderRadius: '50%', border: filter === 'Private regions' ? `2px solid ${color}` : 'none', background: filter === 'Private regions' ? 'transparent' : color}} />)}
            </span>
            {filter}
          </button>;
        })}
        {(activeFilter || regionSearch) && <div style={{display: 'flex', width: '100%', justifyContent: 'flex-start'}}><button type="button" onClick={() => {setActiveFilter(null); setRegionSearch(''); setRegionOpen(false); setOptionIndex(-1); setHovered(null); setFocused(null);}} style={{display: 'inline-flex', alignItems: 'center', gap: 8, padding: '10px 0', border: 0, background: 'transparent', color: 'var(--map-text)', fontSize: 14, fontWeight: 600, cursor: 'pointer'}}>Clear filters <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.25" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="m9 9 6 6m0-6-6 6"/></svg></button></div>}
      </div>
    </section>
  );
};
