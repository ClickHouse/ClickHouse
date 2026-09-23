export const Video = ({ src, width = '100%', autoPlay = true, loop = true, muted = true, playsInline = true, ...rest }) => {
  const base = (typeof window !== 'undefined' && window.location.pathname.startsWith('/docs')) ? '/docs' : '';
  const fullSrc = src && src.startsWith('/') ? base + src : src;
  return (
    <video
      src={fullSrc}
      width={width}
      autoPlay={autoPlay}
      loop={loop}
      muted={muted}
      playsInline={playsInline}
      {...rest}
    />
  );
};